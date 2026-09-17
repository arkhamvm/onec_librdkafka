#ifndef ONEC_TESTS_HOST_COMPONENT_LOADER_H
#define ONEC_TESTS_HOST_COMPONENT_LOADER_H

// Loading and driving librdkafka_onec.so the way the 1C platform does:
//
//   dlopen -> GetClassObject(u"KafkaConsumer", &p)
//   p->Init(addInDefBase) ; p->setMemManager(memoryManager) ; p->RegisterExtensionAs(&name)
//   p->FindMethod(u"Initialize") -> num ; p->CallAsFunc(num, &ret, params, n)
//   p->Done() ; DestroyObject(&p)
//
// ComponentLibrary owns the dlopen handle, ComponentObject owns one component
// instance plus the host stubs it is wired to. Both are RAII; neither is copyable.
//
// Every lookup failure throws HostError with the name that failed - FindMethod
// silently returning -1 is the classic way a test like this goes wrong, so it is
// never allowed to pass unnoticed.

#include <initializer_list>
#include <string>
#include <vector>

#include "ComponentBase.h"
#include "host/onec_host.h"

namespace onec {

class ComponentLibrary
{
public:
    // Throws HostError when the file cannot be dlopen()ed or an export is missing.
    explicit ComponentLibrary(std::string soPath);
    ~ComponentLibrary();

    ComponentLibrary(const ComponentLibrary&) = delete;
    ComponentLibrary& operator=(const ComponentLibrary&) = delete;

    // $ONEC_KAFKA_SO if set, else the ONEC_KAFKA_SO_PATH compile definition,
    // else "out64/librdkafka_onec.so" relative to the working directory.
    static std::string defaultPath();

    const std::string& path() const { return path_; }

    // GetClassNames() split on '|', in declaration order:
    // {"KafkaProducer", "KafkaConsumer", "KafkaAdminClient"}.
    const std::vector<std::string>& classNames() const { return classNames_; }
    bool hasClass(const Name& className) const;

    // Raw GetClassObject / DestroyObject. Prefer ComponentObject.
    IComponentBase* createObject(const Name& className);
    void            destroyObject(IComponentBase*& obj);

    AppCapabilities setPlatformCapabilities(AppCapabilities capabilities);
    AttachType      attachType();

    // librdkafka keeps background threads; leaving the library mapped after the
    // test finishes is sometimes the only way to get a clean exit.
    void setCloseOnDestroy(bool close) { closeOnDestroy_ = close; }

private:
    std::string              path_;
    void*                    handle_ = nullptr;
    bool                     closeOnDestroy_ = true;
    std::vector<std::string> classNames_;

    GetClassObjectPtr            getClassObject_ = nullptr;
    DestroyObjectPtr             destroyObject_ = nullptr;
    GetClassNamesPtr             getClassNames_ = nullptr;
    SetPlatformCapabilitiesPtr   setPlatformCapabilities_ = nullptr;
    GetAttachTypePtr             getAttachType_ = nullptr;
};

// One live component instance, wired to a MemoryManager and a HostAddIn that it
// owns. Construction runs Init / setMemManager / RegisterExtensionAs; destruction
// runs Done / DestroyObject.
class ComponentObject
{
public:
    ComponentObject(ComponentLibrary& library, const Name& className);
    ~ComponentObject();

    ComponentObject(const ComponentObject&) = delete;
    ComponentObject& operator=(const ComponentObject&) = delete;

    const std::string& className() const { return className_; }
    const std::string& registeredName() const { return registeredName_; }  // RegisterExtensionAs
    long               info() const;                                       // GetInfo(), 2000

    HostAddIn&      host() { return host_; }        // AddError / ExternalEvent log
    MemoryManager&  memory() { return memory_; }
    IComponentBase* raw() { return object_; }

    // --- introspection -----------------------------------------------------
    long        methodCount();
    long        propCount();
    long        findMethod(const Name& method);   // -1 when absent, no throw
    long        findProp(const Name& prop);       // -1 when absent, no throw
    long        paramCount(long methodNum);
    bool        hasRetVal(long methodNum);
    std::string methodName(long methodNum, long alias = 0);   // 0 = English, 1 = Russian
    std::string propName(long propNum, long alias = 0);
    std::vector<std::string> methodNames(long alias = 0);
    std::vector<std::string> propNames(long alias = 0);

    // --- calls -------------------------------------------------------------
    // Throw HostError when the method is unknown, when args.size() does not match
    // GetNParams, when the method is not callable that way, or when the component
    // returns false from the dispatch itself.
    Value callFunc(const Name& method, std::initializer_list<Arg> args = {});
    void  callProc(const Name& method, std::initializer_list<Arg> args = {});

    bool        callBool(const Name& method, std::initializer_list<Arg> args = {});
    long        callLong(const Name& method, std::initializer_list<Arg> args = {});
    std::string callString(const Name& method, std::initializer_list<Arg> args = {});

    // --- properties --------------------------------------------------------
    Value getProp(const Name& prop);
    void  setProp(const Name& prop, const Arg& value);

    // Shorthands for the two properties every class carries.
    std::string errorDescription();   // "ErrorDescription"
    bool        fatalError();         // "FatalError" (consumer only)

private:
    Value doCall(const Name& method, const std::vector<Arg>& args, bool asFunction);
    std::string describeMethods();

    ComponentLibrary& library_;
    std::string       className_;
    std::string       registeredName_;
    MemoryManager     memory_;
    HostAddIn         host_;
    IComponentBase*   object_ = nullptr;
};

} // namespace onec

#endif // ONEC_TESTS_HOST_COMPONENT_LOADER_H
