#include "stdafx.h"
#include "ComponentBaseImp.h"

#include <climits>
#include <cstdint>
#include <cstring>
#include <memory>


//---------------------------------------------------------------------------//
ComponentBase::ComponentBase(std::u16string Extension_)
{
	m_iMemory = 0;
	m_iConnect = 0;

	Extension = Extension_;
}
//---------------------------------------------------------------------------//
ComponentBase::~ComponentBase()
{
	PropertyFunction.clear();
	PropertyFunction.shrink_to_fit();

	Propertyes.clear();
	Propertyes.shrink_to_fit();
}
//---------------------------------------------------------------------------//
void ComponentBase::AddFunctionProperty(int _countParam, std::string _Name, std::string _NameRu, FuncPtrType  _pFunction, ProcPtrType  _pProcedure)
{
	sPropertyFunction FunctionProperty;

	FunctionProperty.countParam = _countParam;
	FunctionProperty.itFunction = (_pFunction == 0 ? false : true);
	FunctionProperty.pFunction = _pFunction;
	FunctionProperty.pProcedure = _pProcedure;
	FunctionProperty.Name[0] = _Name;
	FunctionProperty.Name[1] = _NameRu;

	PropertyFunction.push_back(FunctionProperty);
}
//---------------------------------------------------------------------------//
void ComponentBase::AddProperty(std::string _Name, std::string _NameRu, bool IsReadeble, bool IsWriteble)
{
	sProperty Property;

	Property.Name[0] = _Name;
	Property.Name[1] = _NameRu;
	Property.IsReadeble = IsReadeble;
	Property.IsWriteble = IsWriteble;

	Propertyes.push_back(Property);
}
//---------------------------------------------------------------------------//
bool ComponentBase::Init(void* pConnection)
{
	m_iConnect = (IAddInDefBase*)pConnection;
	pAsyncEvent = m_iConnect;
	return m_iConnect != nullptr;
}
//---------------------------------------------------------------------------//
long ComponentBase::GetInfo()
{
	// Component should put supported component technology version
	// This component supports 2.0 version
	return 2000;
}
//---------------------------------------------------------------------------//
void ComponentBase::Done()
{

}
//---------------------------------------------------------------------------//
bool ComponentBase::RegisterExtensionAs(WCHAR_T** wsExtensionName)
{
	if (wsExtensionName == nullptr || m_iMemory == nullptr)
		return false;

	unsigned int iActualSize = (unsigned int)((Extension.size() + 1) * sizeof(WCHAR_T));

	// A failed AllocMemory used to be reported as success, which left the
	// platform reading *wsExtensionName as a name while it was still whatever
	// uninitialised pointer the caller passed in.
	if (!m_iMemory->AllocMemory((void**)wsExtensionName, iActualSize))
		return false;

	memcpy(*wsExtensionName, Extension.c_str(), iActualSize);
	return true;
}
//---------------------------------------------------------------------------//
long ComponentBase::GetNProps()
{
	// You may delete next lines and add your own implementation code here
	return (long)Propertyes.size();
}
//---------------------------------------------------------------------------//
long ComponentBase::FindProp(const WCHAR_T* wsPropName)
{
	long ret = -1;
	std::string prop = toUTF8String(wsPropName, strlen16(wsPropName));
	for (uint32_t i = 0; i < Propertyes.size(); i++)
	{
		for (uint32_t j = 0; j < _CountLanguage; j++)
		{
			if (Propertyes[i].Name[j] == prop)
			{
				ret = i;
				break;
			}
		}
	}
	return ret;
}
//---------------------------------------------------------------------------//
const WCHAR_T* ComponentBase::GetPropName(long lPropNum, long lPropAlias)
{
	if (lPropNum >= (long)Propertyes.size())
		return nullptr;

	if (lPropAlias >= _CountLanguage)
		return 0;

	WCHAR_T* wsPropName = nullptr;
	size_t iActualSize = Propertyes[lPropNum].Name[lPropAlias].size();
	unsigned long alloc_size = (unsigned long)((iActualSize + 1) * sizeof(WCHAR_T));

	if (m_iMemory)
	{
		if (m_iMemory->AllocMemory((void**)&wsPropName, alloc_size)) {
			storeUTF8toUTF16LE(Propertyes[lPropNum].Name[lPropAlias].c_str(), iActualSize, wsPropName, iActualSize + 1);
		}
	}

	return wsPropName;
}
//---------------------------------------------------------------------------//
bool ComponentBase::GetPropVal(const long lPropNum, tVariant* pvarPropVal)
{
	return false;
}
//---------------------------------------------------------------------------//
bool ComponentBase::SetPropVal(const long lPropNum, tVariant* varPropVal)
{
	return false;
}
//---------------------------------------------------------------------------//
bool ComponentBase::IsPropReadable(const long lPropNum)
{
	return Propertyes[lPropNum].IsReadeble;
}
//---------------------------------------------------------------------------//
bool ComponentBase::IsPropWritable(const long lPropNum)
{
	return Propertyes[lPropNum].IsWriteble;
}
//---------------------------------------------------------------------------//
long ComponentBase::GetNMethods()
{
	return (long)PropertyFunction.size();
}
//---------------------------------------------------------------------------//
long ComponentBase::FindMethod(const WCHAR_T* wsMethodName)
{	
	std::string method = toUTF8String(wsMethodName, strlen16(wsMethodName));
	long plMethodNum = -1;
	for (uint32_t i = 0; i < PropertyFunction.size(); i++)
	{
		for (uint32_t j = 0; j < _CountLanguage; j++)
		{
			if (PropertyFunction[i].Name[j] == method)
			{
				plMethodNum = i;
				break;
			}
		}
	}
	return plMethodNum;
}
//---------------------------------------------------------------------------//
const WCHAR_T* ComponentBase::GetMethodName(const long lMethodNum, const long lMethodAlias)
{
	if (lMethodNum >= (long)PropertyFunction.size())
		return NULL;

	if (lMethodAlias >= _CountLanguage)
		return 0;

	WCHAR_T* wsMethodName = nullptr;
	size_t iActualSize = PropertyFunction[lMethodNum].Name[lMethodAlias].size();
	unsigned long alloc_size = (unsigned long)((iActualSize + 1) * sizeof(WCHAR_T));
	if (m_iMemory)
	{
		if (m_iMemory->AllocMemory((void**)&wsMethodName, alloc_size)) {
			storeUTF8toUTF16LE(PropertyFunction[lMethodNum].Name[lMethodAlias].c_str(), iActualSize, wsMethodName, iActualSize + 1);
		}
	}

	return wsMethodName;
}
//---------------------------------------------------------------------------//
long ComponentBase::GetNParams(const long lMethodNum)
{
	if (lMethodNum >= (long)PropertyFunction.size()) return 0;

	return PropertyFunction[lMethodNum].countParam;
}
//---------------------------------------------------------------------------//
bool ComponentBase::GetParamDefValue(const long lMethodNum, const long lParamNum, tVariant* pvarParamDefValue)
{
	TV_VT(pvarParamDefValue) = VTYPE_EMPTY;
	return false;
}
//---------------------------------------------------------------------------//
bool ComponentBase::HasRetVal(const long lMethodNum)
{
	if (lMethodNum >= (long)PropertyFunction.size()) return false;

	return PropertyFunction[lMethodNum].itFunction;
}
//---------------------------------------------------------------------------//
bool ComponentBase::CallAsProc(const long lMethodNum, tVariant* paParams, const long lSizeArray)
{	
	try {
		if (lMethodNum >= (long)PropertyFunction.size()) return false;
		return (PropertyFunction[lMethodNum].pProcedure)(paParams, lSizeArray);
	} catch (...) {
		return false;
	}
}
//---------------------------------------------------------------------------//
bool ComponentBase::CallAsFunc(const long lMethodNum, tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	try {
		if (lMethodNum >= (long)PropertyFunction.size()) return false;
		return (PropertyFunction[lMethodNum].pFunction)(pvarRetValue, paParams, lSizeArray);
	} catch (...) {
		return false;
	}
}
//---------------------------------------------------------------------------//
void ComponentBase::SetLocale(const WCHAR_T* loc)
{
#if !defined( __linux__ )
    //_wsetlocale(LC_ALL, (wchar_t*)loc);
#else
    //We convert in char* char_locale
    //also we establish locale
    //setlocale(LC_ALL, char_locale);
#endif
}
//---------------------------------------------------------------------------//
bool ComponentBase::setMemManager(void* mem)
{
	m_iMemory = (IMemoryManager*)mem;
	return m_iMemory != 0;
}
//---------------------------------------------------------------------------//
void ADDIN_API ComponentBase::SetUserInterfaceLanguageCode(const WCHAR_T* lang)
{
	return;
}
//---------------------------------------------------------------------------//
void ComponentBase::addError(uint32_t wcode, std::string source,
	std::string descriptor, long code)
{
	if (m_iConnect)
	{
		// descriptor must be measured with ITS OWN size. Using source.size()
		// read past the end of a shorter descriptor and silently cut a longer
		// one. Both temporaries live until the end of the full expression, so
		// the pointers handed to AddError stay valid for the whole call.
		m_iConnect->AddError(wcode, toUTF16String(source.c_str(), source.size()).c_str(),
			toUTF16String(descriptor.c_str(), descriptor.size()).c_str(), code);
	}
}
//---------------------------------------------------------------------------//
size_t ComponentBase::strlen16(const WCHAR_T* Source)
{
	if (Source == nullptr)
		return 0;
	return std::char_traits<WCHAR_T>::length(Source);
}
//---------------------------------------------------------------------------//
// Cut a converted buffer at the first NUL inside what was actually produced.
//
// 1C strings are NUL-terminated by convention and the historical behaviour of
// both helpers below was "stop at the first NUL" (they built the result with
// std::string(ptr) / std::u16string(ptr)). That is kept - a caller that hands
// over a length which includes the terminator must keep getting the string
// without it - but it is now bounded by the amount the converter reports, so no
// scan can ever leave the allocation even if it holds no terminator at all.
namespace {

template <typename CharT>
size_t usable_length(const CharT* data, size_t produced)
{
	const CharT* const nul = std::char_traits<CharT>::find(data, produced, CharT(0));
	return (nul != nullptr) ? (size_t)(nul - data) : produced;
}

} // namespace
//---------------------------------------------------------------------------//
// UTF-8 -> UTF-16.
//
// len UTF-8 bytes never decode to more than len UTF-16 code units (a 1..3 byte
// sequence yields one unit, a 4 byte sequence yields two), so len + 1 units is
// a true bound with room for the terminator. That arithmetic was already right;
// what was missing was honouring the result: a failed conversion used to be
// published as whatever happened to be in the buffer.
std::u16string ComponentBase::toUTF16String(const char *src, size_t len)
{
	if (src == nullptr || len == 0)
		return std::u16string();

	if (len > SIZE_MAX - 1)
		return std::u16string();

	const size_t dest_len = len + 1;               // UTF-16 code units
	// new[] and not std::vector / make_unique: the converter zero-fills the
	// buffer itself, and on the hot path a second fill is pure waste.
	std::unique_ptr<WCHAR_T[]> dst(new WCHAR_T[dest_len]);

	size_t produced = 0;
	if (!storeUTF8toUTF16LE(src, len, dst.get(), dest_len, &produced))
		return std::u16string();                   // malformed UTF-8: no guesswork

	return std::u16string((const char16_t*)dst.get(), usable_length(dst.get(), produced));
}
//---------------------------------------------------------------------------//
// UTF-16 -> UTF-8.
//
// A UTF-16 code unit becomes up to THREE UTF-8 bytes (U+0800..U+FFFF - CJK, the
// em dash, the typographic ellipsis, currency signs), and a surrogate pair -
// two units - becomes four, so three bytes per code unit is the true upper
// bound. The old budget was (len + 1) * sizeof(WCHAR_T), i.e. two bytes per
// unit, so any 3-byte character overflowed it: iconv stopped with E2BIG,
// storeUTF16LEtoUTF8 returned false, the result was ignored and the truncated
// buffer was used as the string. Worse, when the truncation landed exactly on
// the last byte of the budget there was no NUL left over from the memset and
// std::string(dst) ran strlen off the end of the allocation (reproduced under
// ASan through Producer1C::SetJSONMessageList).
std::string ComponentBase::toUTF8String(const WCHAR_T *src, size_t len)
{
	if (src == nullptr || len == 0)
		return std::string();

	if (len > (SIZE_MAX - 1) / 3)
		return std::string();

	const size_t dest_bytes = len * 3 + 1;         // bytes, + terminator
	std::unique_ptr<char[]> dst(new char[dest_bytes]);

	size_t produced = 0;
	if (!storeUTF16LEtoUTF8(src, len, dst.get(), dest_bytes, &produced))
		return std::string();                      // malformed UTF-16: no guesswork

	return std::string(dst.get(), usable_length(dst.get(), produced));
}
//---------------------------------------------------------------------------//
// src_len is in bytes, dst_len in UTF-16 code units, *written likewise.
bool ComponentBase::storeUTF8toUTF16LE(const char *src, size_t src_len, WCHAR_T *dst, size_t dst_len,
	size_t *written)
{
	if (written != nullptr)
		*written = 0;

	if (dst == nullptr || dst_len == 0)
		return false;

	size_t dst_len_bytes = dst_len * sizeof(WCHAR_T);
	memset(dst, 0, dst_len_bytes);

	// Nothing to convert. Handled here and not by the platform calls below:
	// MultiByteToWideChar rejects a zero cbMultiByte with
	// ERROR_INVALID_PARAMETER, which made allocString() fail for an empty
	// string on Windows and leave the tVariant without VTYPE_PWSTR/wstrLen.
	if (src == nullptr || src_len == 0)
		return true;

#if defined( __linux__ )
	iconv_t conv = iconv_open("UTF-16LE", "UTF-8");
	if (conv == (iconv_t)-1)
		return false;
	char *_src = (char*)src;
	char *_dst = (char*)dst;
	size_t src_left = src_len;
	size_t dst_left = dst_len_bytes;
	// iconv() returns the number of irreversible conversions (usually 0), never
	// a length, and (size_t)-1 on failure. The amount produced has to be read
	// back from how far the output pointer moved.
	size_t succeed = iconv(conv, &_src, &src_left, &_dst, &dst_left);
	iconv_close(conv);
	if(succeed == (size_t)-1)
		return false;
	if (written != nullptr)
		*written = (dst_len_bytes - dst_left) / sizeof(WCHAR_T);
#else
	// cchWideChar is a COUNT OF CHARACTERS, not bytes, and the return value is
	// the number of characters written - 0 means failure. That is the opposite
	// convention to iconv's, hence the separate handling.
	//
	// MB_ERR_INVALID_CHARS is what makes this branch honour the same contract as
	// the iconv one above. With dwFlags == 0 the call does NOT fail on ill-formed
	// UTF-8 on Vista and later: it substitutes U+FFFD and returns a positive
	// count, so a binary Kafka payload that Linux refuses outright came back on
	// Windows as a populated string full of replacement characters - the same
	// message, silently corrupted on one platform and rejected on the other.
	// Refusing is the direction to converge on for a component whose job is to
	// hand payloads through untouched: a repaired payload cannot be detected by
	// the 1C script, while a refusal is now reported (see allocString) and the
	// script can re-read the batch with base64encode = True.
	// CP_UTF8 + MB_ERR_INVALID_CHARS requires Vista or later, which every
	// platform version this component is built for satisfies.
	if (src_len > (size_t)INT_MAX || dst_len > (size_t)INT_MAX)
		return false;
	int converted = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, src, (int)src_len, (LPWSTR)dst, (int)dst_len);
	if (converted <= 0)
		return false;
	if (written != nullptr)
		*written = (size_t)converted;
#endif
    return true;
}
//---------------------------------------------------------------------------//
// src_len is in UTF-16 code units, dst_len in BYTES, *written likewise.
bool ComponentBase::storeUTF16LEtoUTF8(const WCHAR_T *src, size_t src_len, char *dst, size_t dst_len,
	size_t *written)
{
	if (written != nullptr)
		*written = 0;

	if (dst == nullptr || dst_len == 0)
		return false;

	memset(dst, 0, dst_len);

	// See storeUTF8toUTF16LE: WideCharToMultiByte rejects a zero cchWideChar
	// the same way MultiByteToWideChar rejects a zero cbMultiByte.
	if (src == nullptr || src_len == 0)
		return true;

#if defined( __linux__ )
	iconv_t conv = iconv_open("UTF-8", "UTF-16LE");
	if (conv == (iconv_t)-1)
		return false;
	size_t src_len_bytes = src_len * sizeof(WCHAR_T);
	char *_src = (char*)src;
	char *_dst = dst;
	size_t dst_left = dst_len;
	size_t succeed = iconv(conv, &_src, &src_len_bytes, &_dst, &dst_left);
	iconv_close(conv);
	if(succeed == (size_t)-1)
		return false;
	if (written != nullptr)
		*written = dst_len - dst_left;
#else
	// cchWideChar counts characters, cbMultiByte counts bytes, and the return
	// value is the number of BYTES written - 0 means failure.
	//
	// WC_ERR_INVALID_CHARS is the mirror of MB_ERR_INVALID_CHARS in
	// storeUTF8toUTF16LE: without it an unpaired surrogate coming from 1C is
	// replaced with U+FFFD and reported as success, while iconv fails with
	// EILSEQ. The flag is only valid for CP_UTF8 (and CP_GB18030), which is what
	// is used here, and lpDefaultChar / lpUsedDefaultChar must stay NULL for
	// CP_UTF8 - passing either is ERROR_INVALID_PARAMETER.
	if (src_len > (size_t)INT_MAX || dst_len > (size_t)INT_MAX)
		return false;
	int converted = WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, (LPCWSTR)src, (int)src_len, dst, (int)dst_len, NULL, NULL);
	if (converted <= 0)
		return false;
	if (written != nullptr)
		*written = (size_t)converted;
#endif
    return true;
}
//---------------------------------------------------------------------------//
// Returns false when the string could not be handed over. Every failure used to
// be indistinguishable from an empty string: the caller got VTYPE_EMPTY or an
// empty VTYPE_PWSTR, no error, and - in Consumer1C::ReceiveJSONMessages, whose
// pool the builder has already emptied by then - a whole consumed batch gone.
bool ComponentBase::allocString(tVariant* pvarPropVal, const char* data, size_t src_len,
	std::string* reason)
{
	const auto fail = [reason](const char* text) -> bool {
		if (reason != nullptr)
			*reason = text;
		return false;
	};

	if (reason != nullptr)
		reason->clear();

	if (pvarPropVal == nullptr)
		return fail("no variant to write into");

	// Several callers set TV_VT(pvarPropVal) = VTYPE_PWSTR before calling, so
	// every failure path here has to leave the variant in a state the platform
	// can read: VTYPE_PWSTR over a pwstrVal that was never written is a
	// dangling pointer handed straight back to 1C.
	if (m_iMemory == nullptr) {
		TV_VT(pvarPropVal) = VTYPE_EMPTY;
		return fail("no memory manager");
	}

	// Two ceilings, and one oversized message can hit either: AllocMemory takes
	// an unsigned long, which is 32-bit on Windows, and tVariant::wstrLen is a
	// uint32_t everywhere. Whichever is lower on this target is the limit; a
	// string above it cannot be described to the platform at all, and saying so
	// beats handing over a wrapped length. Only the first ceiling binds on
	// Windows and only the second one on 64-bit Linux, hence the min.
	const size_t alloc_ceiling = (size_t)(ULONG_MAX / sizeof(WCHAR_T)) - 1;
	const size_t wstrlen_ceiling = (size_t)UINT32_MAX - 1;
	const size_t max_src_len = alloc_ceiling < wstrlen_ceiling ? alloc_ceiling : wstrlen_ceiling;

	const size_t dst_len = src_len + 1;             // UTF-16 code units
	if (src_len > max_src_len) {
		TV_VT(pvarPropVal) = VTYPE_EMPTY;
		return fail("string too long for the 1C string ABI");
	}

	if (!m_iMemory->AllocMemory((void**)&pvarPropVal->pwstrVal,
			(unsigned long)(dst_len * sizeof(WCHAR_T)))) {
		// Nothing was allocated, so nothing is left for the platform to free.
		TV_VT(pvarPropVal) = VTYPE_EMPTY;
		return fail("AllocMemory failed");
	}

	size_t written = 0;
	if (!storeUTF8toUTF16LE((const char*)data, src_len, pvarPropVal->pwstrVal, dst_len, &written))
	{
		// The block is the platform's to free now that AllocMemory succeeded,
		// so the variant stays VTYPE_PWSTR - as an empty string, not as the
		// half-converted bytes the buffer may be holding.
		pvarPropVal->pwstrVal[0] = 0;
		TV_VT(pvarPropVal) = VTYPE_PWSTR;
		pvarPropVal->wstrLen = 0;
		return fail("the text is not valid UTF-8");
	}

	TV_VT(pvarPropVal) = VTYPE_PWSTR;
	// What the converter actually produced, not strlen16() of the buffer.
	// wstrLen is the platform's length in characters, and with escaping turned
	// off a raw 0x00 byte from a Kafka payload becomes a U+0000 inside the
	// string: measuring to the first NUL handed 1C everything before it and
	// silently dropped the rest, which the allocation was still holding.
	// "written" can never exceed dst_len - 1, so the zero-filled buffer keeps a
	// terminator inside the allocation for platform code that looks for one.
	pvarPropVal->wstrLen = (uint32_t)written;
	return true;
}
//---------------------------------------------------------------------------//
void ComponentBase::allocBlob(tVariant* pvarPropVal, char* byte_ptr, unsigned int len)
{
	if (!m_iMemory){
		throw std::bad_alloc();
	}

	if (byte_ptr != nullptr){
		if (len > 0){

			if (m_iMemory->AllocMemory((void**)&pvarPropVal->pstrVal, len)){
				TV_VT(pvarPropVal) = VTYPE_BLOB;
				memcpy(pvarPropVal->pstrVal, byte_ptr, len);
				pvarPropVal->strLen = len;
			}
		}
		else{
			pvarPropVal->strLen = 0;
		}
	}
}
//---------------------------------------------------------------------------//
bool ComponentBase::tVariantIsNumber(tVariant* pvarPropVal)
{
	bool res = false;
	if (((pvarPropVal)->vt == VTYPE_UI4) || ((pvarPropVal)->vt == VTYPE_INT) || ((pvarPropVal)->vt == VTYPE_I1) | ((pvarPropVal)->vt == VTYPE_UI1) || ((pvarPropVal)->vt == VTYPE_UI2)
		|| ((pvarPropVal)->vt == VTYPE_I8) || ((pvarPropVal)->vt == VTYPE_UI8) || ((pvarPropVal)->vt == VTYPE_UINT) || ((pvarPropVal)->vt == VTYPE_I2) || ((pvarPropVal)->vt == VTYPE_I4)
		|| ((pvarPropVal)->vt == VTYPE_R4) || ((pvarPropVal)->vt == VTYPE_R8)) {
		res = true;
	}
	return res;
}

//---------------------------------------------------------------------------//


