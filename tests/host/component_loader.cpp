#include "host/component_loader.h"

#include <dlfcn.h>
#include <sys/stat.h>

#include <cstdlib>
#include <sstream>
#include <utility>

namespace onec {

namespace {

std::string dlErrorText()
{
    const char* err = dlerror();
    return err != nullptr ? std::string(err) : std::string("(no dlerror)");
}

template <typename Fn>
Fn resolve(void* handle, const char* symbol, const std::string& path)
{
    dlerror();   // clear
    void* addr = dlsym(handle, symbol);
    if (addr == nullptr) {
        throw HostError("symbol '" + std::string(symbol) + "' not found in " + path
                        + ": " + dlErrorText());
    }
    return reinterpret_cast<Fn>(addr);
}

std::string join(const std::vector<std::string>& items, const char* sep)
{
    std::string out;
    for (std::size_t i = 0; i < items.size(); ++i) {
        if (i != 0) {
            out += sep;
        }
        out += items[i];
    }
    return out;
}

} // namespace

//---------------------------------------------------------------------------//
// ComponentLibrary
//---------------------------------------------------------------------------//

ComponentLibrary::ComponentLibrary(std::string soPath) : path_(std::move(soPath))
{
    dlerror();
    handle_ = dlopen(path_.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (handle_ == nullptr) {
        throw HostError("dlopen(" + path_ + ") failed: " + dlErrorText());
    }

    try {
        getClassObject_          = resolve<GetClassObjectPtr>(handle_, "GetClassObject", path_);
        destroyObject_           = resolve<DestroyObjectPtr>(handle_, "DestroyObject", path_);
        getClassNames_           = resolve<GetClassNamesPtr>(handle_, "GetClassNames", path_);
        setPlatformCapabilities_ = resolve<SetPlatformCapabilitiesPtr>(handle_, "SetPlatformCapabilities", path_);
        getAttachType_           = resolve<GetAttachTypePtr>(handle_, "GetAttachType", path_);
    } catch (...) {
        dlclose(handle_);
        handle_ = nullptr;
        throw;
    }

    // "|KafkaProducer|KafkaConsumer|KafkaAdminClient" -> three names.
    const std::string raw = utf16To8(getClassNames_());
    std::string       current;
    for (const char c : raw) {
        if (c == '|') {
            if (!current.empty()) {
                classNames_.push_back(current);
                current.clear();
            }
        } else {
            current.push_back(c);
        }
    }
    if (!current.empty()) {
        classNames_.push_back(current);
    }
}

ComponentLibrary::~ComponentLibrary()
{
    if (handle_ != nullptr && closeOnDestroy_) {
        dlclose(handle_);
    }
    handle_ = nullptr;
}

std::string ComponentLibrary::defaultPath()
{
    if (const char* fromEnv = std::getenv("ONEC_KAFKA_SO")) {
        if (fromEnv[0] != '\0') {
            return std::string(fromEnv);
        }
    }
#ifdef ONEC_KAFKA_SO_PATH
    return std::string(ONEC_KAFKA_SO_PATH);
#else
    // Whichever of these exists, so the test runs from the repo root, from
    // tests/ or from a build directory without being told where the .so is.
    static const char* const candidates[] = {
        "out64/librdkafka_onec.so",
        "../out64/librdkafka_onec.so",
        "../../out64/librdkafka_onec.so",
    };
    for (const char* candidate : candidates) {
        struct stat info;
        if (::stat(candidate, &info) == 0 && S_ISREG(info.st_mode)) {
            return std::string(candidate);
        }
    }
    return std::string(candidates[0]);   // let dlopen produce the error message
#endif
}

bool ComponentLibrary::hasClass(const Name& className) const
{
    const std::string wanted = className.utf8();
    for (const std::string& name : classNames_) {
        if (name == wanted) {
            return true;
        }
    }
    return false;
}

IComponentBase* ComponentLibrary::createObject(const Name& className)
{
    // GetClassObject only fills *pInterface when it recognises the name, and it
    // reports 1 either way - so the out pointer is the real result.
    IComponentBase* object = nullptr;
    getClassObject_(className.c_str(), &object);
    if (object == nullptr) {
        throw HostError("GetClassObject(\"" + className.utf8() + "\") produced no object; "
                        + path_ + " exports {" + join(classNames_, ", ") + "}");
    }
    return object;
}

void ComponentLibrary::destroyObject(IComponentBase*& obj)
{
    if (obj == nullptr) {
        return;
    }
    destroyObject_(&obj);
    obj = nullptr;
}

AppCapabilities ComponentLibrary::setPlatformCapabilities(AppCapabilities capabilities)
{
    return setPlatformCapabilities_(capabilities);
}

AttachType ComponentLibrary::attachType()
{
    return getAttachType_();
}

//---------------------------------------------------------------------------//
// ComponentObject
//---------------------------------------------------------------------------//

ComponentObject::ComponentObject(ComponentLibrary& library, const Name& className)
    : library_(library), className_(className.utf8())
{
    object_ = library_.createObject(className);

    // Exactly the order the platform uses.
    if (!object_->Init(static_cast<IAddInDefBase*>(&host_))) {
        library_.destroyObject(object_);
        throw HostError(className_ + ": Init() returned false");
    }
    // setMemManager takes void*, and the component casts it straight back to
    // IMemoryManager* (ComponentBase::setMemManager, src/ComponentBaseImp.cpp:246).
    // The upcast has to happen here: passing a derived pointer through void*
    // only works as long as the base sits at offset 0, which is not something
    // the ABI promises.
    if (!object_->setMemManager(static_cast<IMemoryManager*>(&memory_))) {
        library_.destroyObject(object_);
        throw HostError(className_ + ": setMemManager() returned false");
    }

    WCHAR_T* extensionName = nullptr;
    if (!object_->RegisterExtensionAs(&extensionName) || extensionName == nullptr) {
        library_.destroyObject(object_);
        throw HostError(className_ + ": RegisterExtensionAs() returned no name");
    }
    registeredName_ = utf16To8(extensionName);
    void* buffer = extensionName;
    memory_.FreeMemory(&buffer);
}

ComponentObject::~ComponentObject()
{
    if (object_ != nullptr) {
        object_->Done();
        library_.destroyObject(object_);
    }
}

long ComponentObject::info() const
{
    return object_->GetInfo();
}

long ComponentObject::methodCount()
{
    return object_->GetNMethods();
}

long ComponentObject::propCount()
{
    return object_->GetNProps();
}

long ComponentObject::findMethod(const Name& method)
{
    return object_->FindMethod(method.c_str());
}

long ComponentObject::findProp(const Name& prop)
{
    return object_->FindProp(prop.c_str());
}

long ComponentObject::paramCount(long methodNum)
{
    return object_->GetNParams(methodNum);
}

bool ComponentObject::hasRetVal(long methodNum)
{
    return object_->HasRetVal(methodNum);
}

std::string ComponentObject::methodName(long methodNum, long alias)
{
    if (methodNum < 0 || methodNum >= methodCount()) {
        throw HostError(className_ + ": method index " + std::to_string(methodNum)
                        + " out of range (GetNMethods() == "
                        + std::to_string(methodCount()) + ")");
    }
    const WCHAR_T* name = object_->GetMethodName(methodNum, alias);
    if (name == nullptr) {
        return std::string();
    }
    const std::string result = utf16To8(name);
    void* buffer = const_cast<WCHAR_T*>(name);
    memory_.FreeMemory(&buffer);
    return result;
}

std::string ComponentObject::propName(long propNum, long alias)
{
    if (propNum < 0 || propNum >= propCount()) {
        throw HostError(className_ + ": property index " + std::to_string(propNum)
                        + " out of range (GetNProps() == "
                        + std::to_string(propCount()) + ")");
    }
    const WCHAR_T* name = object_->GetPropName(propNum, alias);
    if (name == nullptr) {
        return std::string();
    }
    const std::string result = utf16To8(name);
    void* buffer = const_cast<WCHAR_T*>(name);
    memory_.FreeMemory(&buffer);
    return result;
}

std::vector<std::string> ComponentObject::methodNames(long alias)
{
    std::vector<std::string> names;
    const long count = methodCount();
    names.reserve(static_cast<std::size_t>(count > 0 ? count : 0));
    for (long i = 0; i < count; ++i) {
        names.push_back(methodName(i, alias));
    }
    return names;
}

std::vector<std::string> ComponentObject::propNames(long alias)
{
    std::vector<std::string> names;
    const long count = propCount();
    names.reserve(static_cast<std::size_t>(count > 0 ? count : 0));
    for (long i = 0; i < count; ++i) {
        names.push_back(propName(i, alias));
    }
    return names;
}

std::string ComponentObject::describeMethods()
{
    return join(methodNames(0), ", ");
}

Value ComponentObject::doCall(const Name& method, const std::vector<Arg>& args, bool asFunction)
{
    const std::string methodUtf8 = method.utf8();

    const long methodNum = object_->FindMethod(method.c_str());
    if (methodNum < 0) {
        throw HostError(className_ + "::" + methodUtf8
                        + ": FindMethod returned -1 (unknown method). Known methods: "
                        + describeMethods());
    }

    const long expected = object_->GetNParams(methodNum);
    if (expected != static_cast<long>(args.size())) {
        std::ostringstream os;
        os << className_ << "::" << methodUtf8 << " (index " << methodNum
           << ") takes " << expected << " parameter(s), " << args.size() << " given";
        throw HostError(os.str());
    }

    const bool isFunction = object_->HasRetVal(methodNum);
    if (asFunction && !isFunction) {
        throw HostError(className_ + "::" + methodUtf8
                        + " is registered as a procedure (HasRetVal == false); use callProc");
    }
    if (!asFunction && isFunction) {
        throw HostError(className_ + "::" + methodUtf8
                        + " is registered as a function (HasRetVal == true); use callFunc");
    }

    // One spare slot so paParams is never null for a zero-argument method.
    std::vector<tVariant> params(args.size() + 1);
    for (std::size_t i = 0; i < params.size(); ++i) {
        varInit(params[i]);
    }
    try {
        for (std::size_t i = 0; i < args.size(); ++i) {
            args[i].materialize(params[i], memory_);
        }
    } catch (...) {
        for (tVariant& p : params) {
            varFree(p, memory_);
        }
        throw;
    }

    const std::size_t errorsBefore = host_.errorCount();

    Value result(&memory_);
    bool  ok = false;
    try {
        if (asFunction) {
            ok = object_->CallAsFunc(methodNum, result.ptr(), params.data(),
                                     static_cast<long>(args.size()));
        } else {
            ok = object_->CallAsProc(methodNum, params.data(),
                                     static_cast<long>(args.size()));
        }
    } catch (...) {
        for (tVariant& p : params) {
            varFree(p, memory_);
        }
        throw;
    }

    for (tVariant& p : params) {
        varFree(p, memory_);
    }

    if (!ok) {
        std::ostringstream os;
        os << className_ << "::" << methodUtf8 << " (index " << methodNum << ") returned false";
        if (host_.errorCount() > errorsBefore) {
            os << "; AddError: " << host_.lastError().str();
        }
        throw HostError(os.str());
    }
    return result;
}

Value ComponentObject::callFunc(const Name& method, std::initializer_list<Arg> args)
{
    return doCall(method, std::vector<Arg>(args), true);
}

void ComponentObject::callProc(const Name& method, std::initializer_list<Arg> args)
{
    doCall(method, std::vector<Arg>(args), false);
}

bool ComponentObject::callBool(const Name& method, std::initializer_list<Arg> args)
{
    const Value value = callFunc(method, args);
    try {
        return value.asBool();
    } catch (const HostError& e) {
        throw HostError(className_ + "::" + method.utf8() + ": " + e.what());
    }
}

long ComponentObject::callLong(const Name& method, std::initializer_list<Arg> args)
{
    const Value value = callFunc(method, args);
    try {
        return value.asLong();
    } catch (const HostError& e) {
        throw HostError(className_ + "::" + method.utf8() + ": " + e.what());
    }
}

std::string ComponentObject::callString(const Name& method, std::initializer_list<Arg> args)
{
    const Value value = callFunc(method, args);
    try {
        return value.asString();
    } catch (const HostError& e) {
        throw HostError(className_ + "::" + method.utf8() + ": " + e.what());
    }
}

Value ComponentObject::getProp(const Name& prop)
{
    const std::string propUtf8 = prop.utf8();

    const long propNum = object_->FindProp(prop.c_str());
    if (propNum < 0) {
        throw HostError(className_ + "." + propUtf8
                        + ": FindProp returned -1 (unknown property). Known properties: "
                        + join(propNames(0), ", "));
    }
    if (!object_->IsPropReadable(propNum)) {
        throw HostError(className_ + "." + propUtf8 + " is not readable");
    }

    Value result(&memory_);
    if (!object_->GetPropVal(propNum, result.ptr())) {
        throw HostError(className_ + "." + propUtf8 + ": GetPropVal (index "
                        + std::to_string(propNum) + ") returned false");
    }
    return result;
}

void ComponentObject::setProp(const Name& prop, const Arg& value)
{
    const std::string propUtf8 = prop.utf8();

    const long propNum = object_->FindProp(prop.c_str());
    if (propNum < 0) {
        throw HostError(className_ + "." + propUtf8
                        + ": FindProp returned -1 (unknown property). Known properties: "
                        + join(propNames(0), ", "));
    }
    if (!object_->IsPropWritable(propNum)) {
        throw HostError(className_ + "." + propUtf8 + " is not writable");
    }

    tVariant variant;
    varInit(variant);
    value.materialize(variant, memory_);

    bool ok = false;
    try {
        ok = object_->SetPropVal(propNum, &variant);
    } catch (...) {
        varFree(variant, memory_);
        throw;
    }
    varFree(variant, memory_);

    if (!ok) {
        throw HostError(className_ + "." + propUtf8 + " = " + value.describe()
                        + ": SetPropVal (index " + std::to_string(propNum)
                        + ") returned false");
    }
}

std::string ComponentObject::errorDescription()
{
    return getProp(u"ErrorDescription").asString();
}

bool ComponentObject::fatalError()
{
    return getProp(u"FatalError").asBool();
}

} // namespace onec
