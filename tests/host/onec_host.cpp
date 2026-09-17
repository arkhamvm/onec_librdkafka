#include "host/onec_host.h"

#include <cstdlib>
#include <cstring>
#include <limits>
#include <ostream>
#include <sstream>

namespace onec {

namespace {

const char32_t kReplacement = 0xFFFDu;

void appendUtf16(std::u16string& out, char32_t cp)
{
    if (cp < 0x10000u) {
        out.push_back(static_cast<char16_t>(cp));
        return;
    }
    cp -= 0x10000u;
    out.push_back(static_cast<char16_t>(0xD800u + (cp >> 10)));
    out.push_back(static_cast<char16_t>(0xDC00u + (cp & 0x3FFu)));
}

void appendUtf8(std::string& out, char32_t cp)
{
    if (cp < 0x80u) {
        out.push_back(static_cast<char>(cp));
    } else if (cp < 0x800u) {
        out.push_back(static_cast<char>(0xC0u | (cp >> 6)));
        out.push_back(static_cast<char>(0x80u | (cp & 0x3Fu)));
    } else if (cp < 0x10000u) {
        out.push_back(static_cast<char>(0xE0u | (cp >> 12)));
        out.push_back(static_cast<char>(0x80u | ((cp >> 6) & 0x3Fu)));
        out.push_back(static_cast<char>(0x80u | (cp & 0x3Fu)));
    } else {
        out.push_back(static_cast<char>(0xF0u | (cp >> 18)));
        out.push_back(static_cast<char>(0x80u | ((cp >> 12) & 0x3Fu)));
        out.push_back(static_cast<char>(0x80u | ((cp >> 6) & 0x3Fu)));
        out.push_back(static_cast<char>(0x80u | (cp & 0x3Fu)));
    }
}

std::string quote(const std::string& s)
{
    return "\"" + s + "\"";
}

// Every numeric parameter the component actually reads out of a tVariant is
// 32 bits wide: Consumer1C::ConsumePool reads paParams->uintVal,
// QueryWatermarkOffsets and CommittedOffset read (paParams+1)->lVal and
// (paParams+2)->lVal, Producer1C::Initialize reads (paParams+2)->lVal, and
// AddRecordToTopicPartitionList reads (paParams+1)->lVal.
// ComponentBase::tVariantIsNumber accepts VTYPE_I8 as well, so a 64-bit variant
// sails through the component's own type check and is then read as the low half
// of the union - right on little-endian x86_64, wrong anywhere else, and silent
// in both cases. So a value that fits in int32_t always travels as VTYPE_I4,
// and VTYPE_I8 is reserved for values that genuinely do not fit. Arg::i64()
// still forces VTYPE_I8 for tests that want exactly that.
bool fitsInt32(std::int64_t value)
{
    return value >= static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::min())
        && value <= static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::max());
}

} // namespace

//---------------------------------------------------------------------------//
// UTF conversion
//---------------------------------------------------------------------------//

std::u16string utf8To16(const std::string& utf8)
{
    std::u16string out;
    out.reserve(utf8.size());

    const unsigned char* p   = reinterpret_cast<const unsigned char*>(utf8.data());
    const unsigned char* end = p + utf8.size();

    while (p < end) {
        const unsigned char lead = *p;
        char32_t    cp    = 0;
        std::size_t extra = 0;

        if (lead < 0x80u) {
            cp = lead;
            extra = 0;
        } else if ((lead & 0xE0u) == 0xC0u) {
            cp = lead & 0x1Fu;
            extra = 1;
        } else if ((lead & 0xF0u) == 0xE0u) {
            cp = lead & 0x0Fu;
            extra = 2;
        } else if ((lead & 0xF8u) == 0xF0u) {
            cp = lead & 0x07u;
            extra = 3;
        } else {
            // Stray continuation byte or 5/6-byte form: drop one byte.
            ++p;
            appendUtf16(out, kReplacement);
            continue;
        }

        if (static_cast<std::size_t>(end - p) < extra + 1) {
            ++p;
            appendUtf16(out, kReplacement);
            continue;
        }

        bool ok = true;
        for (std::size_t i = 1; i <= extra; ++i) {
            const unsigned char cont = p[i];
            if ((cont & 0xC0u) != 0x80u) {
                ok = false;
                break;
            }
            cp = (cp << 6) | (cont & 0x3Fu);
        }
        if (!ok) {
            ++p;
            appendUtf16(out, kReplacement);
            continue;
        }
        p += extra + 1;

        const bool overlong = (extra == 1 && cp < 0x80u)
                           || (extra == 2 && cp < 0x800u)
                           || (extra == 3 && cp < 0x10000u);
        if (overlong || cp > 0x10FFFFu || (cp >= 0xD800u && cp <= 0xDFFFu)) {
            cp = kReplacement;
        }
        appendUtf16(out, cp);
    }
    return out;
}

std::string utf16To8(const std::u16string& utf16)
{
    std::string out;
    out.reserve(utf16.size());

    const std::size_t n = utf16.size();
    for (std::size_t i = 0; i < n; ++i) {
        char32_t cp = static_cast<char32_t>(static_cast<std::uint16_t>(utf16[i]));
        if (cp >= 0xD800u && cp <= 0xDBFFu) {
            if (i + 1 < n) {
                const char32_t low = static_cast<char32_t>(static_cast<std::uint16_t>(utf16[i + 1]));
                if (low >= 0xDC00u && low <= 0xDFFFu) {
                    cp = 0x10000u + ((cp - 0xD800u) << 10) + (low - 0xDC00u);
                    ++i;
                } else {
                    cp = kReplacement;
                }
            } else {
                cp = kReplacement;
            }
        } else if (cp >= 0xDC00u && cp <= 0xDFFFu) {
            cp = kReplacement;
        }
        appendUtf8(out, cp);
    }
    return out;
}

std::string utf16To8(const WCHAR_T* src, std::size_t lenInCodeUnits)
{
    if (src == nullptr) {
        return std::string();
    }
    return utf16To8(std::u16string(reinterpret_cast<const char16_t*>(src), lenInCodeUnits));
}

std::string utf16To8(const WCHAR_T* zeroTerminated)
{
    if (zeroTerminated == nullptr) {
        return std::string();
    }
    return utf16To8(zeroTerminated, u16len(zeroTerminated));
}

std::size_t u16len(const WCHAR_T* zeroTerminated)
{
    if (zeroTerminated == nullptr) {
        return 0;
    }
    const char16_t* p = reinterpret_cast<const char16_t*>(zeroTerminated);
    std::size_t n = 0;
    while (p[n] != u'\0') {
        ++n;
    }
    return n;
}

//---------------------------------------------------------------------------//
// MemoryManager
//---------------------------------------------------------------------------//

MemoryManager::~MemoryManager()
{
    // Whatever the component forgot to hand back is released here so the test
    // process itself stays clean; liveBlocks() is the thing tests assert on.
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& entry : blocks_) {
        std::free(entry.first);
    }
    blocks_.clear();
}

bool ADDIN_API MemoryManager::AllocMemory(void** pMemory, unsigned long ulCountByte)
{
    if (pMemory == nullptr) {
        return false;
    }
    {
        // Armed by failNextAllocations(); off unless a test asked for it. The
        // refusal happens before malloc so nothing is allocated and nothing has
        // to be cleaned up - which is exactly what the platform does.
        std::lock_guard<std::mutex> lock(mutex_);
        if (failNextAllocs_ != 0) {
            --failNextAllocs_;
            ++refusedAllocs_;
            return false;
        }
    }
    *pMemory = std::malloc(ulCountByte != 0 ? ulCountByte : 1);
    if (*pMemory == nullptr) {
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    blocks_[*pMemory] = ulCountByte;
    ++totalBlocks_;
    return true;
}

void ADDIN_API MemoryManager::FreeMemory(void** pMemory)
{
    if (pMemory == nullptr || *pMemory == nullptr) {
        return;
    }

    void* const block = *pMemory;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const std::map<void*, std::size_t>::iterator it = blocks_.find(block);
        if (it == blocks_.end()) {
            // Not a block this manager issued (or one already freed). Record it
            // and hand it back untouched: free()ing it would corrupt the test
            // process's heap and turn a reportable contract violation into an
            // unrelated crash somewhere else. *pMemory is deliberately left
            // pointing at the offending block so the caller can still inspect it.
            ++foreignFrees_;
            if (violations_.size() < kMaxRecordedViolations) {
                violations_.push_back(block);
            }
            return;
        }
        blocks_.erase(it);
    }
    std::free(block);
    *pMemory = nullptr;
}

std::size_t MemoryManager::liveBlocks() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return blocks_.size();
}

std::size_t MemoryManager::liveBytes() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    std::size_t total = 0;
    for (const auto& entry : blocks_) {
        total += entry.second;
    }
    return total;
}

std::size_t MemoryManager::totalBlocks() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return totalBlocks_;
}

std::size_t MemoryManager::foreignFrees() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return foreignFrees_;
}

std::vector<void*> MemoryManager::violations() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return violations_;
}

std::string MemoryManager::violationReport() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (foreignFrees_ == 0) {
        return "no foreign FreeMemory() calls";
    }

    std::ostringstream os;
    os << foreignFrees_ << " foreign FreeMemory() call(s): the component handed back"
          " a pointer that did not come from AllocMemory (or freed one twice):";
    for (std::size_t i = 0; i < violations_.size(); ++i) {
        os << (i == 0 ? " " : ", ") << violations_[i];
    }
    if (violations_.size() < foreignFrees_) {
        os << ", ... (" << (foreignFrees_ - violations_.size()) << " more not recorded)";
    }
    return os.str();
}

void MemoryManager::clearViolations()
{
    std::lock_guard<std::mutex> lock(mutex_);
    foreignFrees_ = 0;
    violations_.clear();
}

void MemoryManager::failNextAllocations(std::size_t count)
{
    std::lock_guard<std::mutex> lock(mutex_);
    failNextAllocs_ = count;
}

std::size_t MemoryManager::pendingAllocFailures() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return failNextAllocs_;
}

std::size_t MemoryManager::refusedAllocations() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return refusedAllocs_;
}

//---------------------------------------------------------------------------//
// Records
//---------------------------------------------------------------------------//

std::string ErrorRecord::str() const
{
    std::ostringstream os;
    os << "wcode=" << wcode
       << " source=" << quote(source)
       << " descr=" << quote(descr)
       << " scode=" << scode;
    return os.str();
}

std::string EventRecord::str() const
{
    std::ostringstream os;
    os << "source=" << quote(source)
       << " message=" << quote(message)
       << " data=" << quote(data);
    return os.str();
}

//---------------------------------------------------------------------------//
// HostAddIn
//---------------------------------------------------------------------------//

HostAddIn::HostAddIn() : msgBoxIface_(this) {}

HostAddIn::~HostAddIn() = default;

bool ADDIN_API HostAddIn::AddError(unsigned short wcode, const WCHAR_T* source,
                                   const WCHAR_T* descr, long scode)
{
    // The component passes c_str() of temporaries (ComponentBase::addError),
    // so the strings have to be copied right here.
    ErrorRecord rec;
    rec.wcode  = wcode;
    rec.source = utf16To8(source);
    rec.descr  = utf16To8(descr);
    rec.scode  = scode;

    std::lock_guard<std::mutex> lock(mutex_);
    errors_.push_back(std::move(rec));
    return true;
}

bool ADDIN_API HostAddIn::Read(WCHAR_T* /*wszPropName*/, tVariant* pVal,
                               long* pErrCode, WCHAR_T** errDescriptor)
{
    if (pVal != nullptr) {
        varInit(*pVal);
    }
    if (pErrCode != nullptr) {
        *pErrCode = 0;
    }
    if (errDescriptor != nullptr) {
        *errDescriptor = nullptr;
    }
    return false;   // the harness exposes no platform-side properties
}

bool ADDIN_API HostAddIn::Write(WCHAR_T* /*wszPropName*/, tVariant* /*pVar*/)
{
    return false;
}

bool ADDIN_API HostAddIn::RegisterProfileAs(WCHAR_T* wszProfileName)
{
    std::lock_guard<std::mutex> lock(mutex_);
    profileName_ = utf16To8(wszProfileName);
    return true;
}

bool ADDIN_API HostAddIn::SetEventBufferDepth(long lDepth)
{
    std::lock_guard<std::mutex> lock(mutex_);
    eventBufferDepth_ = lDepth;
    return true;
}

long ADDIN_API HostAddIn::GetEventBufferDepth()
{
    std::lock_guard<std::mutex> lock(mutex_);
    return eventBufferDepth_;
}

bool ADDIN_API HostAddIn::ExternalEvent(WCHAR_T* wszSource, WCHAR_T* wszMessage,
                                        WCHAR_T* wszData)
{
    EventRecord rec;
    rec.source  = utf16To8(wszSource);
    rec.message = utf16To8(wszMessage);
    rec.data    = utf16To8(wszData);

    {
        std::lock_guard<std::mutex> lock(mutex_);
        events_.push_back(std::move(rec));
    }
    eventCv_.notify_all();
    return true;
}

void ADDIN_API HostAddIn::CleanEventBuffer()
{
    clearEvents();
}

bool ADDIN_API HostAddIn::SetStatusLine(WCHAR_T* wszStatusLine)
{
    std::lock_guard<std::mutex> lock(mutex_);
    statusLine_ = utf16To8(wszStatusLine);
    return true;
}

void ADDIN_API HostAddIn::ResetStatusLine()
{
    std::lock_guard<std::mutex> lock(mutex_);
    statusLine_.clear();
}

IInterface* ADDIN_API HostAddIn::GetInterface(Interfaces iface)
{
    if (iface == eIMsgBox) {
        return static_cast<IInterface*>(&msgBoxIface_);
    }
    return nullptr;
}

bool ADDIN_API HostAddIn::MsgBox::Confirm(const WCHAR_T* queryText, tVariant* retVal)
{
    owner_->record(MsgBoxRecord{"Confirm", utf16To8(queryText)});
    if (retVal != nullptr) {
        varSetBool(*retVal, true);
    }
    return true;
}

bool ADDIN_API HostAddIn::MsgBox::Alert(const WCHAR_T* text)
{
    owner_->record(MsgBoxRecord{"Alert", utf16To8(text)});
    return true;
}

void HostAddIn::record(MsgBoxRecord rec)
{
    std::lock_guard<std::mutex> lock(mutex_);
    msgBox_.push_back(std::move(rec));
}

std::vector<ErrorRecord> HostAddIn::errors() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return errors_;
}

std::vector<EventRecord> HostAddIn::events() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return events_;
}

std::vector<MsgBoxRecord> HostAddIn::msgBoxCalls() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return msgBox_;
}

std::size_t HostAddIn::errorCount() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return errors_.size();
}

std::size_t HostAddIn::eventCount() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return events_.size();
}

ErrorRecord HostAddIn::lastError() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return errors_.empty() ? ErrorRecord() : errors_.back();
}

EventRecord HostAddIn::lastEvent() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return events_.empty() ? EventRecord() : events_.back();
}

void HostAddIn::clearErrors()
{
    std::lock_guard<std::mutex> lock(mutex_);
    errors_.clear();
}

void HostAddIn::clearEvents()
{
    std::lock_guard<std::mutex> lock(mutex_);
    events_.clear();
}

void HostAddIn::clearAll()
{
    std::lock_guard<std::mutex> lock(mutex_);
    errors_.clear();
    events_.clear();
    msgBox_.clear();
    statusLine_.clear();
}

bool HostAddIn::waitForEvents(std::size_t atLeast, std::chrono::milliseconds timeout) const
{
    std::unique_lock<std::mutex> lock(mutex_);
    return eventCv_.wait_for(lock, timeout, [&] { return events_.size() >= atLeast; });
}

std::string HostAddIn::statusLine() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return statusLine_;
}

std::string HostAddIn::profileName() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return profileName_;
}

long HostAddIn::eventBufferDepth() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return eventBufferDepth_;
}

//---------------------------------------------------------------------------//
// tVariant
//---------------------------------------------------------------------------//

void varInit(tVariant& v)
{
    std::memset(&v, 0, sizeof(tVariant));
    v.vt = VTYPE_EMPTY;
}

void varSetBool(tVariant& v, bool value)
{
    varInit(v);
    v.bVal = value;
    v.vt   = VTYPE_BOOL;
}

void varSetInt32(tVariant& v, std::int32_t v32)
{
    varInit(v);
    v.lVal = v32;                 // lVal and intVal are the same 4 bytes
    v.vt   = VTYPE_I4;
}

void varSetUInt32(tVariant& v, std::uint32_t u)
{
    varInit(v);
    v.ulVal = u;
    v.vt    = VTYPE_UI4;
}

void varSetInt64(tVariant& v, std::int64_t v64)
{
    varInit(v);
    v.llVal = v64;
    v.vt    = VTYPE_I8;
}

void varSetDouble(tVariant& v, double value)
{
    varInit(v);
    v.dblVal = value;
    v.vt     = VTYPE_R8;
}

void varSetString(tVariant& v, const std::string& utf8, IMemoryManager& mm)
{
    varInit(v);

    const std::u16string wide = utf8To16(utf8);
    const unsigned long  bytes =
        static_cast<unsigned long>((wide.size() + 1) * sizeof(WCHAR_T));

    void* buffer = nullptr;
    if (!mm.AllocMemory(&buffer, bytes)) {
        throw HostError("varSetString: AllocMemory failed for "
                        + std::to_string(bytes) + " bytes");
    }
    std::memcpy(buffer, wide.c_str(), bytes);

    v.pwstrVal = static_cast<WCHAR_T*>(buffer);
    v.wstrLen  = static_cast<std::uint32_t>(wide.size());   // code units, no terminator
    v.vt       = VTYPE_PWSTR;
}

void varFree(tVariant& v, IMemoryManager& mm)
{
    switch (v.vt & VTYPE_TYPEMASK) {
        case VTYPE_PWSTR:
            if (v.pwstrVal != nullptr) {
                void* p = v.pwstrVal;
                mm.FreeMemory(&p);
                v.pwstrVal = nullptr;
            }
            break;
        case VTYPE_PSTR:
        case VTYPE_BLOB:
            if (v.pstrVal != nullptr) {
                void* p = v.pstrVal;
                mm.FreeMemory(&p);
                v.pstrVal = nullptr;
            }
            break;
        default:
            break;
    }
    varInit(v);
}

bool varIsNumber(const tVariant& v)
{
    switch (v.vt) {
        case VTYPE_I1:
        case VTYPE_I2:
        case VTYPE_I4:
        case VTYPE_I8:
        case VTYPE_UI1:
        case VTYPE_UI2:
        case VTYPE_UI4:
        case VTYPE_UI8:
        case VTYPE_INT:
        case VTYPE_UINT:
        case VTYPE_R4:
        case VTYPE_R8:
            return true;
        default:
            return false;
    }
}

std::string varTypeName(TYPEVAR vt)
{
    switch (vt) {
        case VTYPE_EMPTY:     return "VTYPE_EMPTY";
        case VTYPE_NULL:      return "VTYPE_NULL";
        case VTYPE_I2:        return "VTYPE_I2";
        case VTYPE_I4:        return "VTYPE_I4";
        case VTYPE_R4:        return "VTYPE_R4";
        case VTYPE_R8:        return "VTYPE_R8";
        case VTYPE_DATE:      return "VTYPE_DATE";
        case VTYPE_TM:        return "VTYPE_TM";
        case VTYPE_PSTR:      return "VTYPE_PSTR";
        case VTYPE_INTERFACE: return "VTYPE_INTERFACE";
        case VTYPE_ERROR:     return "VTYPE_ERROR";
        case VTYPE_BOOL:      return "VTYPE_BOOL";
        case VTYPE_VARIANT:   return "VTYPE_VARIANT";
        case VTYPE_I1:        return "VTYPE_I1";
        case VTYPE_UI1:       return "VTYPE_UI1";
        case VTYPE_UI2:       return "VTYPE_UI2";
        case VTYPE_UI4:       return "VTYPE_UI4";
        case VTYPE_I8:        return "VTYPE_I8";
        case VTYPE_UI8:       return "VTYPE_UI8";
        case VTYPE_INT:       return "VTYPE_INT";
        case VTYPE_UINT:      return "VTYPE_UINT";
        case VTYPE_HRESULT:   return "VTYPE_HRESULT";
        case VTYPE_PWSTR:     return "VTYPE_PWSTR";
        case VTYPE_BLOB:      return "VTYPE_BLOB";
        case VTYPE_CLSID:     return "VTYPE_CLSID";
        default:              break;
    }
    return "VTYPE(" + std::to_string(static_cast<unsigned>(vt)) + ")";
}

std::string varDescribe(const tVariant& v)
{
    std::ostringstream os;
    os << varTypeName(v.vt) << "(";
    switch (v.vt) {
        case VTYPE_EMPTY:
        case VTYPE_NULL:
            break;
        case VTYPE_BOOL:
            os << (v.bVal ? "true" : "false");
            break;
        case VTYPE_R4:
            os << v.fltVal;
            break;
        case VTYPE_R8:
        case VTYPE_DATE:
            os << v.dblVal;
            break;
        case VTYPE_PWSTR:
            os << quote(varToString(v)) << ", wstrLen=" << v.wstrLen;
            break;
        case VTYPE_PSTR:
        case VTYPE_BLOB:
            os << quote(varToString(v)) << ", strLen=" << v.strLen;
            break;
        default:
            if (varIsNumber(v)) {
                os << varToInt(v);
            } else {
                os << "?";
            }
            break;
    }
    os << ")";
    return os.str();
}

bool varToBool(const tVariant& v)
{
    if (v.vt != VTYPE_BOOL) {
        throw HostError("expected VTYPE_BOOL, got " + varDescribe(v));
    }
    return v.bVal;
}

std::int64_t varToInt(const tVariant& v)
{
    switch (v.vt) {
        case VTYPE_I1:   return v.i8Val;
        case VTYPE_I2:   return v.shortVal;
        case VTYPE_I4:   return v.lVal;
        case VTYPE_I8:   return v.llVal;
        case VTYPE_UI1:  return v.ui8Val;
        case VTYPE_UI2:  return v.ushortVal;
        case VTYPE_UI4:  return v.ulVal;
        case VTYPE_UI8:  return static_cast<std::int64_t>(v.ullVal);
        case VTYPE_INT:  return v.intVal;
        case VTYPE_UINT: return v.uintVal;
        case VTYPE_R4:   return static_cast<std::int64_t>(v.fltVal);
        case VTYPE_R8:   return static_cast<std::int64_t>(v.dblVal);
        default:
            throw HostError("expected a numeric variant, got " + varDescribe(v));
    }
}

double varToDouble(const tVariant& v)
{
    if (v.vt == VTYPE_R4) {
        return v.fltVal;
    }
    if (v.vt == VTYPE_R8 || v.vt == VTYPE_DATE) {
        return v.dblVal;
    }
    return static_cast<double>(varToInt(v));
}

std::string varToString(const tVariant& v)
{
    switch (v.vt) {
        case VTYPE_PWSTR:
            if (v.pwstrVal == nullptr) {
                return std::string();
            }
            return utf16To8(v.pwstrVal, v.wstrLen);
        case VTYPE_PSTR:
        case VTYPE_BLOB:
            if (v.pstrVal == nullptr) {
                return std::string();
            }
            return std::string(v.pstrVal, v.strLen);
        case VTYPE_EMPTY:
            return std::string();
        default:
            throw HostError("expected a string variant, got " + varDescribe(v));
    }
}

//---------------------------------------------------------------------------//
// Arg
//---------------------------------------------------------------------------//

Arg::Arg() = default;
Arg::Arg(bool value) : kind_(Kind::Bool), bool_(value) {}
Arg::Arg(int value) : kind_(Kind::I32), i32_(value) {}
Arg::Arg(unsigned int value) : kind_(Kind::U32), u32_(value) {}

// The four wide integer overloads narrow to VTYPE_I4 whenever the value fits;
// see fitsInt32() above for why that matters. Writing 1000L, or passing a
// size_t or a .size(), therefore produces the same variant as the int literal
// 1000 rather than a VTYPE_I8 the component reads half of.
Arg::Arg(long long value)
{
    if (fitsInt32(value)) {
        kind_ = Kind::I32;
        i32_  = static_cast<std::int32_t>(value);
    } else {
        kind_ = Kind::I64;
        i64_  = value;
    }
}

Arg::Arg(unsigned long long value)
{
    if (value <= static_cast<unsigned long long>(std::numeric_limits<std::int32_t>::max())) {
        kind_ = Kind::I32;
        i32_  = static_cast<std::int32_t>(value);
    } else {
        kind_ = Kind::I64;
        i64_  = static_cast<std::int64_t>(value);
    }
}

Arg::Arg(long value) : Arg(static_cast<long long>(value)) {}
Arg::Arg(unsigned long value) : Arg(static_cast<unsigned long long>(value)) {}

Arg::Arg(double value) : kind_(Kind::F64), f64_(value) {}
Arg::Arg(const char* utf8) : kind_(Kind::Str), str_(utf8 != nullptr ? utf8 : "") {}
Arg::Arg(std::string utf8) : kind_(Kind::Str), str_(std::move(utf8)) {}
Arg::Arg(const std::u16string& utf16) : kind_(Kind::Str), str_(utf16To8(utf16)) {}

Arg Arg::empty() { return Arg(); }
Arg Arg::boolean(bool value) { return Arg(value); }
Arg Arg::i32(std::int32_t value) { Arg a; a.kind_ = Kind::I32; a.i32_ = value; return a; }
Arg Arg::u32(std::uint32_t value) { Arg a; a.kind_ = Kind::U32; a.u32_ = value; return a; }
Arg Arg::i64(std::int64_t value) { Arg a; a.kind_ = Kind::I64; a.i64_ = value; return a; }
Arg Arg::f64(double value) { return Arg(value); }
Arg Arg::str(std::string utf8) { return Arg(std::move(utf8)); }

void Arg::materialize(tVariant& out, IMemoryManager& mm) const
{
    switch (kind_) {
        case Kind::Empty: varInit(out); break;
        case Kind::Bool:  varSetBool(out, bool_); break;
        case Kind::I32:   varSetInt32(out, i32_); break;
        case Kind::U32:   varSetUInt32(out, u32_); break;
        case Kind::I64:   varSetInt64(out, i64_); break;
        case Kind::F64:   varSetDouble(out, f64_); break;
        case Kind::Str:   varSetString(out, str_, mm); break;
    }
}

std::string Arg::describe() const
{
    std::ostringstream os;
    switch (kind_) {
        case Kind::Empty: os << "empty"; break;
        case Kind::Bool:  os << (bool_ ? "true" : "false"); break;
        case Kind::I32:   os << i32_; break;
        case Kind::U32:   os << u32_ << "u"; break;
        case Kind::I64:   os << i64_ << "LL"; break;
        case Kind::F64:   os << f64_; break;
        case Kind::Str:   os << quote(str_); break;
    }
    return os.str();
}

//---------------------------------------------------------------------------//
// Value
//---------------------------------------------------------------------------//

Value::Value()
{
    varInit(var_);
}

Value::Value(MemoryManager* mm) : mm_(mm)
{
    varInit(var_);
}

Value::~Value()
{
    reset();
}

Value::Value(Value&& other) noexcept : mm_(other.mm_), var_(other.var_)
{
    varInit(other.var_);
    other.mm_ = nullptr;
}

Value& Value::operator=(Value&& other) noexcept
{
    if (this != &other) {
        reset();
        mm_  = other.mm_;
        var_ = other.var_;
        varInit(other.var_);
        other.mm_ = nullptr;
    }
    return *this;
}

void Value::reset()
{
    if (mm_ != nullptr) {
        varFree(var_, *mm_);
    } else {
        varInit(var_);
    }
}

bool Value::isEmpty() const { return var_.vt == VTYPE_EMPTY; }
bool Value::isBool() const { return var_.vt == VTYPE_BOOL; }
bool Value::isString() const { return var_.vt == VTYPE_PWSTR || var_.vt == VTYPE_PSTR || var_.vt == VTYPE_BLOB; }
bool Value::isNumber() const { return varIsNumber(var_); }

bool         Value::asBool() const { return varToBool(var_); }
long         Value::asLong() const { return static_cast<long>(varToInt(var_)); }
std::int64_t Value::asInt64() const { return varToInt(var_); }
double       Value::asDouble() const { return varToDouble(var_); }
std::string  Value::asString() const { return varToString(var_); }
std::string  Value::describe() const { return varDescribe(var_); }

std::ostream& operator<<(std::ostream& os, const Value& value)
{
    return os << value.describe();
}

} // namespace onec
