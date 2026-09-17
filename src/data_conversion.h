#ifndef __DATACONVERSION_H__
#define __DATACONVERSION_H__

#include "component_types.h"
#include "nlohmann/json.hpp"
#include "base64.h"
#include <inttypes.h>

namespace KafkaExport
{
	namespace DataConversion
	{
		class DataConteiner
		{
		public:
			bool DecodeBase64Key = false;
			bool DecodeBase64Value = false;
			bool DecodeBase64HeadersValue = false;
			bool CheckJSONFieldsForStringType = false;
		private:
			nlohmann::json json;
			bool checked = false;

			bool JSONStructureCheck();

		public:
			DataConteiner();
			virtual ~DataConteiner();
			RetValue Load(std::string JSON);
			void Clear();
			bool IsValid();

			size_t ElementsCount();
			bool GetKeyByIndex(uint32_t index, std::string* dest);
			bool GetValueByIndex(uint32_t index, std::string* dest);
			bool GetHeadersByIndex(uint32_t index, RdKafka::Headers* dest);
			bool HeadersContains(uint32_t index);
			size_t GetHeadersCountByIndex(uint32_t index);
		};

		class DataBuilder
		{
		public:
			std::set<std::string> HeaderFilter;
			bool RemoveMessagesFromLocalQueueOnBuild = false;
			bool EscapeMessageValue = true;
			bool EscapeMessageKey = true;
			bool EscapeMessageHeaderValue = true;
			bool EscapeMessageHeaderKey = true;

			DataBuilder();
			virtual ~DataBuilder();
			StringValueResult JSON_DeliveryReport(std::vector<KafkaExport::delivery_record> records, bool DecodeBase64Key = false);
			KafkaExport::StringValueResult JSON_TopicPartitionList(std::vector<KafkaExport::TopicPartitionDescription>* records);
			KafkaExport::StringValueResult JSON_GroupList(std::vector<KafkaExport::GroupDescription>* records);
			KafkaExport::StringValueResult JSON_GetJSONWatermarkOffsets(KafkaExport::RetOffsets offsets);
			KafkaExport::StringValueResult JSON_KafkaMessagePool(std::vector<RdKafka::Message*>* local_queue, bool base64encode);
			KafkaExport::StringValueResult JSON_Metadata(KafkaExport::MetadataDescription *metadata);
			KafkaExport::StringValueResult JSON_WatermarkOffsets(std::vector<KafkaExport::WatermarkOffsets>* records);
			KafkaExport::StringValueResult OnecInternal_KafkaMessagePool(std::vector<RdKafka::Message*>* local_queue, bool binary_data);
			KafkaExport::RetValue AppendHeaderFilter(std::string heder_key);
			KafkaExport::RetValue ClearHeaderFilter();

		private:
			// Sentinel for "no length supplied, measure the NUL-terminated string yourself".
			// It used to be 0, which collided with a length that is legitimately zero: a Kafka
			// tombstone has payload() == nullptr and len() == 0, so the helpers below called
			// strlen(nullptr) and segfaulted, and an empty-but-non-null value made strlen run
			// forward into the neighbouring records of the same fetch buffer and splice their
			// bytes into the JSON handed to 1C.
			static const size_t kMeasureLength = static_cast<size_t>(-1);

			// No kMeasureLength default here, unlike the append_* helpers below. This one
			// is the raw escaper: every caller already holds the byte count of the buffer
			// it is escaping, and a defaulted sentinel would be a trap rather than a
			// convenience - the sentinel is SIZE_MAX, so a call that forgot the length
			// would walk the heap until it faulted. Making the length mandatory lets the
			// compiler reject that call instead.
			void escape_string_simple(std::string* buffer, const char* data, size_t len);
			void append_key_value_string(std::string* buffer, const char* key, const char* data, size_t len = kMeasureLength);
			void append_key_value_value(std::string* buffer, const char* key, const char* data, size_t len = kMeasureLength);
			// "<key>":<token> for a payload the caller asked NOT to escape, where the
			// payload may be absent or empty. See the definition: absent becomes null,
			// present-but-empty becomes "". Length is mandatory for the same reason as
			// escape_string_simple above - Kafka always hands one over.
			void append_key_value_raw_json(std::string* buffer, const char* key, const char* data, size_t len);
			void append_key_value_escaped_string(std::string* buffer, const char* key, const char* data, size_t len = kMeasureLength);
			void append_key_value_base64_encoded_string(std::string* buffer, const char* key, const unsigned char* data, size_t len = kMeasureLength);
			
			void internal_escape_string_simple(std::string* buffer, const char* data, size_t len);
			void internal_clmnstr(std::string* buffer, const char* value, size_t len = kMeasureLength, bool comma = true);
			void internal_clmnbin(std::string* buffer, const unsigned char* value, size_t len = kMeasureLength, bool comma = true);
			void internal_clmnnum(std::string* buffer, const char* value, size_t len = kMeasureLength);			
		};		
	}
}
#endif
