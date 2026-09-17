#ifndef ONEC_TESTS_HOST_ONEC_HOST_H
#define ONEC_TESTS_HOST_ONEC_HOST_H

// Mini 1C:Enterprise host: the platform-side objects a Native API component
// expects to be handed at load time.
//
//   * MemoryManager - IMemoryManager over malloc/free, with leak accounting.
//   * HostAddIn     - IAddInDefBaseEx (+ IMsgBox); AddError and ExternalEvent
//                     are recorded, everything else is a stub. librdkafka calls
//                     back from its own threads, so the logs are mutex-protected.
//   * UTF-8 <-> UTF-16 conversion, hand rolled (std::wstring_convert is
//                     deprecated in C++17 and gone in C++26).
//   * tVariant plumbing: Arg (host -> component) and Value (component -> host).
//
// The tVariant contract mirrors what the component actually does, see
// src/ComponentBaseImp.cpp (allocString/toUTF8String/tVariantIsNumber) and the
// per-method argument checks in src/consumer1c.cpp.

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include "types.h"
#include "AddInDefBase.h"
#include "IMemoryManager.h"

namespace onec {

//---------------------------------------------------------------------------//
// Errors
//---------------------------------------------------------------------------//

// Thrown by everything in this harness that cannot go on: an unresolved symbol,
// an unknown method name, an arity mismatch, a tVariant of the wrong type.
class HostError : public std::runtime_error
{
public:
    explicit HostError(const std::string& what) : std::runtime_error(what) {}
};

//---------------------------------------------------------------------------//
// UTF-8 <-> UTF-16 (WCHAR_T is char16_t on Linux, include/types.h:71)
//---------------------------------------------------------------------------//

// Invalid input is replaced with U+FFFD rather than rejected: the point is to
// keep a test readable, not to validate encodings.
std::u16string utf8To16(const std::string& utf8);
std::string    utf16To8(const std::u16string& utf16);
std::string    utf16To8(const WCHAR_T* src, std::size_t lenInCodeUnits);
std::string    utf16To8(const WCHAR_T* zeroTerminated);
std::size_t    u16len(const WCHAR_T* zeroTerminated);

// A method / property / class name written either way: u"Subscribe" or
// "Subscribe". Everything crossing the ABI is UTF-16.
struct Name
{
    std::u16string wide;

    Name(const char16_t* s) : wide(s ? s : u"") {}          // NOLINT(google-explicit-constructor)
    Name(std::u16string s) : wide(std::move(s)) {}          // NOLINT(google-explicit-constructor)
    Name(const char* s) : wide(utf8To16(s ? s : "")) {}     // NOLINT(google-explicit-constructor)
    Name(const std::string& s) : wide(utf8To16(s)) {}       // NOLINT(google-explicit-constructor)

    const WCHAR_T* c_str() const { return wide.c_str(); }
    std::string    utf8() const { return utf16To8(wide); }
};

//---------------------------------------------------------------------------//
// IMemoryManager
//---------------------------------------------------------------------------//

// The component allocates every string it hands back (ComponentBase::allocString,
// GetMethodName, GetPropName, RegisterExtensionAs) through this object, and the
// host is the one that frees it. liveBlocks() is therefore a real leak check.
//
// FreeMemory() never frees a pointer it did not issue. A component that hands
// back memory it did not get from AllocMemory - the .c_str() of an internal
// u16string, a static buffer, a new[] block - is the classic Native API bug,
// and it is exactly what this object exists to catch. Freeing such a pointer
// would corrupt the test process's own heap instead of reporting the bug, and
// would silently drop nothing from blocks_, so liveBlocks() would keep looking
// clean. Those calls are refused, counted in foreignFrees() and listed in
// violations() instead. A double free lands in the same bucket: the second call
// no longer finds the block.
class MemoryManager final : public IMemoryManager
{
public:
    MemoryManager() = default;
    ~MemoryManager() override;

    MemoryManager(const MemoryManager&) = delete;
    MemoryManager& operator=(const MemoryManager&) = delete;

    bool ADDIN_API AllocMemory(void** pMemory, unsigned long ulCountByte) override;
    void ADDIN_API FreeMemory(void** pMemory) override;

    std::size_t liveBlocks() const;   // allocated and not yet freed
    std::size_t liveBytes() const;
    std::size_t totalBlocks() const;  // allocated since construction

    // --- contract violations -----------------------------------------------
    // CHECK_EQ(object.memory().foreignFrees(), std::size_t(0));
    // CHECK_MSG(object.memory().foreignFrees() == 0,
    //           object.memory().violationReport());
    std::size_t        foreignFrees() const;      // FreeMemory() calls refused
    std::vector<void*> violations() const;        // the pointers, capped, in order
    std::string        violationReport() const;   // one line, ready for a message
    void               clearViolations();

    // --- fault injection ---------------------------------------------------
    // Make the next `count` AllocMemory() calls answer false without allocating
    // anything, exactly as the 1C platform does when it cannot satisfy the
    // request. The component is required to survive that on every string it
    // hands back, and - since the fix for finding 4 - to say so through
    // ErrorDescription instead of returning an empty string with Success.
    //
    // A real out-of-memory condition cannot be provoked from a test, and this
    // is the only failure mode of allocString() that needs no broker, so it is
    // the cheapest guard there is. Off by default; nothing else in the suite
    // arms it.
    void        failNextAllocations(std::size_t count);
    std::size_t pendingAllocFailures() const;
    std::size_t refusedAllocations() const;       // how many were actually refused

private:
    // Only the pointer list is capped; foreignFrees_ keeps counting past it.
    static constexpr std::size_t kMaxRecordedViolations = 32;

    mutable std::mutex              mutex_;
    std::map<void*, std::size_t>    blocks_;
    std::size_t                     totalBlocks_ = 0;
    std::size_t                     foreignFrees_ = 0;
    std::size_t                     failNextAllocs_ = 0;
    std::size_t                     refusedAllocs_ = 0;
    std::vector<void*>              violations_;
};

//---------------------------------------------------------------------------//
// IAddInDefBase
//---------------------------------------------------------------------------//

struct ErrorRecord
{
    unsigned short wcode = 0;
    std::string    source;
    std::string    descr;
    long           scode = 0;

    std::string str() const;   // "wcode=N source='..' descr='..' scode=N"
};

struct EventRecord
{
    std::string source;
    std::string message;
    std::string data;

    std::string str() const;   // "source='..' message='..' data='..'"
};

struct MsgBoxRecord
{
    std::string kind;   // "Confirm" or "Alert"
    std::string text;
};

// Stub platform. Only AddError and ExternalEvent keep anything; the rest do the
// least surprising thing and return success.
class HostAddIn final : public IAddInDefBaseEx
{
public:
    HostAddIn();
    ~HostAddIn() override;

    HostAddIn(const HostAddIn&) = delete;
    HostAddIn& operator=(const HostAddIn&) = delete;

    // --- IAddInDefBase -----------------------------------------------------
    bool ADDIN_API AddError(unsigned short wcode, const WCHAR_T* source,
                            const WCHAR_T* descr, long scode) override;
    bool ADDIN_API Read(WCHAR_T* wszPropName, tVariant* pVal, long* pErrCode,
                        WCHAR_T** errDescriptor) override;
    bool ADDIN_API Write(WCHAR_T* wszPropName, tVariant* pVar) override;
    bool ADDIN_API RegisterProfileAs(WCHAR_T* wszProfileName) override;
    bool ADDIN_API SetEventBufferDepth(long lDepth) override;
    long ADDIN_API GetEventBufferDepth() override;
    bool ADDIN_API ExternalEvent(WCHAR_T* wszSource, WCHAR_T* wszMessage,
                                 WCHAR_T* wszData) override;
    void ADDIN_API CleanEventBuffer() override;
    bool ADDIN_API SetStatusLine(WCHAR_T* wszStatusLine) override;
    void ADDIN_API ResetStatusLine() override;

    // --- IAddInDefBaseEx ---------------------------------------------------
    IInterface* ADDIN_API GetInterface(Interfaces iface) override;

    // --- host side ---------------------------------------------------------
    std::vector<ErrorRecord>  errors() const;
    std::vector<EventRecord>  events() const;
    std::vector<MsgBoxRecord> msgBoxCalls() const;

    std::size_t errorCount() const;
    std::size_t eventCount() const;

    // Default-constructed record when nothing was logged.
    ErrorRecord lastError() const;
    EventRecord lastEvent() const;

    void clearErrors();
    void clearEvents();
    void clearAll();

    // Blocks until eventCount() >= atLeast or the timeout expires. Kafka events
    // arrive on librdkafka's own threads, so a test cannot just look once.
    bool waitForEvents(std::size_t atLeast, std::chrono::milliseconds timeout) const;

    std::string statusLine() const;    // last SetStatusLine text
    std::string profileName() const;   // last RegisterProfileAs name
    long        eventBufferDepth() const;

private:
    // Returned from GetInterface(eIMsgBox); records into the owning HostAddIn.
    class MsgBox final : public IMsgBox
    {
    public:
        explicit MsgBox(HostAddIn* owner) : owner_(owner) {}
        bool ADDIN_API Confirm(const WCHAR_T* queryText, tVariant* retVal) override;
        bool ADDIN_API Alert(const WCHAR_T* text) override;
    private:
        HostAddIn* owner_;
    };

    void record(MsgBoxRecord rec);

    mutable std::mutex              mutex_;
    mutable std::condition_variable eventCv_;
    std::vector<ErrorRecord>        errors_;
    std::vector<EventRecord>        events_;
    std::vector<MsgBoxRecord>       msgBox_;
    std::string                     statusLine_;
    std::string                     profileName_;
    long                            eventBufferDepth_ = 1000;
    MsgBox                          msgBoxIface_;
};

//---------------------------------------------------------------------------//
// tVariant
//---------------------------------------------------------------------------//

void varInit(tVariant& v);                      // zero + VTYPE_EMPTY
void varSetBool(tVariant& v, bool value);       // VTYPE_BOOL  -> bVal
void varSetInt32(tVariant& v, std::int32_t v32);// VTYPE_I4    -> lVal (== intVal)
void varSetUInt32(tVariant& v, std::uint32_t u);// VTYPE_UI4   -> ulVal
void varSetInt64(tVariant& v, std::int64_t v64);// VTYPE_I8    -> llVal
void varSetDouble(tVariant& v, double value);   // VTYPE_R8    -> dblVal

// VTYPE_PWSTR. The buffer comes from the memory manager, exactly like the one
// the platform would hand the component, and wstrLen counts UTF-16 code units
// without the terminator (ComponentBase::allocString does the same).
void varSetString(tVariant& v, const std::string& utf8, IMemoryManager& mm);

// Releases a PWSTR / PSTR / BLOB payload through the memory manager and resets
// the variant to VTYPE_EMPTY. A no-op for scalars.
void varFree(tVariant& v, IMemoryManager& mm);

// Mirrors ComponentBase::tVariantIsNumber - the check the component itself runs
// on numeric parameters.
bool varIsNumber(const tVariant& v);

std::string varTypeName(TYPEVAR vt);
std::string varDescribe(const tVariant& v);     // "VTYPE_BOOL(true)", for messages

bool         varToBool(const tVariant& v);      // throws HostError on a type mismatch
std::int64_t varToInt(const tVariant& v);       // accepts any numeric variant
double       varToDouble(const tVariant& v);
std::string  varToString(const tVariant& v);    // PWSTR -> UTF-8, PSTR/BLOB -> raw bytes

//---------------------------------------------------------------------------//
// Arg - a value on its way into the component
//---------------------------------------------------------------------------//

// Implicitly constructible so a call reads like the 1C source it stands in for:
//   consumer.callFunc(u"Initialize", {"kafka:9093", "group-1"});
//   consumer.callFunc(u"ConsumePool", {1000, 10, 0});
//   consumer.callFunc(u"ReceiveJSONMessages", {false});
class Arg
{
public:
    Arg();                                                  // VTYPE_EMPTY
    Arg(bool value);                                        // NOLINT(google-explicit-constructor)
    Arg(int value);                                         // VTYPE_I4
    Arg(unsigned int value);                                // VTYPE_UI4 (ulVal == uintVal)

    // The wide integer overloads deliberately do NOT hard-code VTYPE_I8: a value
    // that fits in int32_t becomes VTYPE_I4, and only a value that does not
    // becomes VTYPE_I8. Every numeric parameter the component reads is 32-bit
    // (Consumer1C::ConsumePool -> paParams->uintVal, QueryWatermarkOffsets and
    // CommittedOffset -> (paParams+1)->lVal / (paParams+2)->lVal,
    // Producer1C::Initialize -> (paParams+2)->lVal,
    // AddRecordToTopicPartitionList -> (paParams+1)->lVal), while
    // ComponentBase::tVariantIsNumber happily accepts VTYPE_I8 - so a 64-bit
    // variant passes the component's type check and is then read as the low 32
    // bits of the union. Correct on little-endian x86_64, wrong on a big-endian
    // target, and warning-free either way. Narrowing here means 1000L, a size_t
    // and a .size() all behave exactly like the literal 1000.
    Arg(long value);                                        // NOLINT(google-explicit-constructor)
    Arg(unsigned long value);                               // NOLINT(google-explicit-constructor)
    Arg(long long value);                                   // NOLINT(google-explicit-constructor)
    Arg(unsigned long long value);                          // NOLINT(google-explicit-constructor)

    Arg(double value);                                      // NOLINT(google-explicit-constructor)
    Arg(const char* utf8);                                  // NOLINT(google-explicit-constructor)
    Arg(std::string utf8);                                  // NOLINT(google-explicit-constructor)
    Arg(const std::u16string& utf16);                       // NOLINT(google-explicit-constructor)

    // Explicit spellings, for when the implicit one would be ambiguous or unclear.
    // These do not narrow - i64() is how a test asks for a VTYPE_I8 variant on
    // purpose, for instance to prove the component's own type check accepts it.
    static Arg empty();
    static Arg boolean(bool value);
    static Arg i32(std::int32_t value);
    static Arg u32(std::uint32_t value);
    static Arg i64(std::int64_t value);
    static Arg f64(double value);
    static Arg str(std::string utf8);

    // Fills out; a string argument allocates through mm and must be released
    // with varFree(). ComponentObject does that for you.
    void materialize(tVariant& out, IMemoryManager& mm) const;

    std::string describe() const;

private:
    enum class Kind { Empty, Bool, I32, U32, I64, F64, Str };

    Kind          kind_ = Kind::Empty;
    bool          bool_ = false;
    std::int32_t  i32_  = 0;
    std::uint32_t u32_  = 0;
    std::int64_t  i64_  = 0;
    double        f64_  = 0.0;
    std::string   str_;
};

//---------------------------------------------------------------------------//
// Value - a value on its way out of the component
//---------------------------------------------------------------------------//

// Owns whatever the component allocated into the variant and frees it on
// destruction, so a test never has to think about the memory manager.
class Value
{
public:
    Value();
    explicit Value(MemoryManager* mm);
    ~Value();

    Value(Value&& other) noexcept;
    Value& operator=(Value&& other) noexcept;
    Value(const Value&) = delete;
    Value& operator=(const Value&) = delete;

    tVariant*       ptr() { return &var_; }          // the slot CallAsFunc writes into
    const tVariant& raw() const { return var_; }
    TYPEVAR         type() const { return var_.vt; }

    bool isEmpty() const;
    bool isBool() const;
    bool isString() const;
    bool isNumber() const;

    bool         asBool() const;     // throws HostError unless VTYPE_BOOL
    long         asLong() const;     // any numeric variant
    std::int64_t asInt64() const;
    double       asDouble() const;
    std::string  asString() const;   // PWSTR -> UTF-8, PSTR/BLOB -> raw bytes

    std::string describe() const;    // "VTYPE_PWSTR(\"...\")"
    void        reset();             // release the payload, back to VTYPE_EMPTY

private:
    MemoryManager* mm_ = nullptr;
    tVariant       var_{};
};

std::ostream& operator<<(std::ostream& os, const Value& value);

} // namespace onec

#endif // ONEC_TESTS_HOST_ONEC_HOST_H
