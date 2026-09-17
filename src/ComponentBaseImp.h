#ifndef __COMPONENTBASEIMP_H__
#define __COMPONENTBASEIMP_H__

#include <functional>
#include <string>
#include <vector>
#include <string_view>
#include "ComponentBase.h"
#include "AddInDefBase.h"
#include "IMemoryManager.h"

#if defined( __linux__ )
#include <iconv.h>
#endif

#if !defined( __linux__ )
#include <windows.h>
#endif


const int _CountLanguage = 2;

class ComponentBase : public IComponentBase
{

public:

	typedef std::function<bool(tVariant*, tVariant*, const long)> FuncPtrType;
	typedef std::function<bool(tVariant*, const long)> ProcPtrType;

    struct sPropertyFunction
	{
		int countParam = 0;
		bool itFunction = false;
		std::string Name[_CountLanguage];
		FuncPtrType  pFunction;
		ProcPtrType  pProcedure;
	};

	struct sProperty
	{
		std::string Name[_CountLanguage];
		bool IsReadeble = false;
		bool IsWriteble = false;
	};
	

	
	ComponentBase(std::u16string Extension_);
	virtual ~ComponentBase();
	// IInitDoneBase
	virtual bool ADDIN_API Init(void*);
	virtual bool ADDIN_API setMemManager(void* mem);
	virtual long ADDIN_API GetInfo();
	virtual void ADDIN_API Done();
	// ILanguageExtenderBase
	virtual bool ADDIN_API RegisterExtensionAs(WCHAR_T**);
	virtual long ADDIN_API GetNProps();
	virtual long ADDIN_API FindProp(const WCHAR_T* wsPropName);
	virtual const WCHAR_T* ADDIN_API GetPropName(long lPropNum, long lPropAlias);
	virtual bool ADDIN_API GetPropVal(const long lPropNum, tVariant* pvarPropVal);
	virtual bool ADDIN_API SetPropVal(const long lPropNum, tVariant* varPropVal);
	virtual bool ADDIN_API IsPropReadable(const long lPropNum);
	virtual bool ADDIN_API IsPropWritable(const long lPropNum);
	virtual long ADDIN_API GetNMethods();
	virtual long ADDIN_API FindMethod(const WCHAR_T* wsMethodName);
	virtual const WCHAR_T* ADDIN_API GetMethodName(const long lMethodNum, const long lMethodAlias);
	virtual long ADDIN_API GetNParams(const long lMethodNum);
	virtual bool ADDIN_API GetParamDefValue(const long lMethodNum, const long lParamNum, tVariant* pvarParamDefValue);
	virtual bool ADDIN_API HasRetVal(const long lMethodNum);
	virtual bool ADDIN_API CallAsProc(const long lMethodNum, tVariant* paParams, const long lSizeArray);
	virtual bool ADDIN_API CallAsFunc(const long lMethodNum, tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
	// LocaleBase
	virtual void ADDIN_API SetLocale(const WCHAR_T* loc);
	// UserLanguageBase
    virtual void ADDIN_API SetUserInterfaceLanguageCode(const WCHAR_T* lang) override;

protected:

	IAddInDefBase* m_iConnect = nullptr;
	IMemoryManager* m_iMemory = nullptr;
	IAddInDefBase* pAsyncEvent = nullptr;

	std::vector<sPropertyFunction> PropertyFunction;
	std::vector<sProperty> Propertyes;
	std::u16string Extension;

	long findName(std::wstring name) const;

	// Publishes data[0..src_len) as a VTYPE_PWSTR the platform will free.
	//
	// Returns false when the value could NOT be published: no memory manager, a
	// string too long for the 1C string ABI, a refused AllocMemory, or a
	// conversion the platform rejected (ill-formed UTF-8). The variant is always
	// left in a state the platform can read - VTYPE_EMPTY when nothing was
	// allocated, VTYPE_PWSTR over an empty string when the block exists but
	// could not be filled - so a caller may ignore the result, but a caller
	// whose data is gone the moment it returns must not: an empty string with an
	// empty ErrorDescription is indistinguishable from an empty answer.
	// "reason", when given, receives a short English description of the failure.
	bool allocString(tVariant* pvarPropVal, const char* data, size_t src_len,
		std::string* reason = nullptr);
	void allocBlob(tVariant* pvarPropVal, char *byte_ptr, unsigned int len);
	void addError(uint32_t wcode, std::string source, std::string descriptor, long code);
	void AddFunctionProperty(int _countParam, std::string _Name, std::string _NameRu, FuncPtrType _pFunction, ProcPtrType _pProcedure);
	void AddProperty(std::string _Name, std::string _NameRu, bool IsReadeble, bool IsWriteble);
	bool tVariantIsNumber(tVariant* pvarPropVal);
	size_t strlen16(const WCHAR_T* Source);

	// Both converters zero-fill the destination, refuse to write past it and
	// report failure instead of leaving a half-converted buffer behind.
	// Ill-formed input is a failure on EVERY platform: iconv reports EILSEQ, and
	// the Win32 calls are given MB_ERR_INVALID_CHARS / WC_ERR_INVALID_CHARS,
	// without which they would quietly substitute U+FFFD and report success.
	// Mind the asymmetric units, they match the platform calls underneath:
	//   storeUTF8toUTF16LE  dst_len is in UTF-16 CODE UNITS  (written likewise)
	//   storeUTF16LEtoUTF8  dst_len is in BYTES              (written likewise)
	// "written" is optional and receives the amount actually produced, which is
	// the only reliable length when the text may contain U+0000.
	bool storeUTF8toUTF16LE(const char *src, size_t src_len, WCHAR_T *dst, size_t dst_len,
		size_t *written = nullptr);
	bool storeUTF16LEtoUTF8(const WCHAR_T *src, size_t src_len, char *dst, size_t dst_len,
		size_t *written = nullptr);

	// An empty string is returned when the conversion genuinely fails (malformed
	// UTF-16 / UTF-8, an unpaired surrogate): a truncated, plausible-looking
	// string is the one result a caller cannot defend against. This holds on
	// Windows as well - see the flags mentioned above - so the two platforms
	// answer the same way for the same bytes, and a string that comes back is
	// always a faithful transcription of the input, never a repair of it.
   	std::string toUTF8String(const WCHAR_T *src, size_t len);
	std::u16string toUTF16String(const char *src, size_t len);
};

#endif
