#include "stdafx.h"
#include "producer1c.h"

using namespace KafkaExport::KafkaProducerc1C;
//---------------------------------------------------------------------------//
namespace {
// The broker list comes straight from 1C and is echoed back in ErrorDescription,
// which the platform copies through IMemoryManager. Cap it so a pathological
// argument cannot turn one error message into a multi-megabyte allocation.
// Kept byte-identical in producer1c_core.cpp, consumer1c_core.cpp and
// admin_client1c_core.cpp so the three classes report the same thing.
const size_t kMaxBrokersInMessage = 200;

std::string BrokersForMessage(const std::string& brokers)
{
	if (brokers.size() <= kMaxBrokersInMessage) {
		return brokers;
	}

	// The cut has to land on a UTF-8 character boundary. The finished message goes
	// through allocString(), which converts it to UTF-16 with iconv, and a trailing
	// half sequence makes iconv stop with EILSEQ - the script would then read an
	// EMPTY ErrorDescription instead of "no usable broker address in ...", so the
	// diagnostic would destroy itself exactly when a non-ASCII broker string is what
	// went wrong. Continuation bytes are 10xxxxxx and a UTF-8 sequence is at most
	// four bytes long, so stepping back over at most three of them always reaches
	// the start of the character that straddles the limit: well-formed input stays
	// well-formed. Input that is already ill-formed cannot be repaired here and is
	// left as it is rather than silently rewritten.
	size_t cut = kMaxBrokersInMessage;
	const size_t min_cut = cut > 3 ? cut - 3 : 0;
	while ((cut > min_cut) && ((static_cast<unsigned char>(brokers[cut]) & 0xC0) == 0x80)) {
		--cut;
	}

	return brokers.substr(0, cut) + "...";
}
} // namespace
//---------------------------------------------------------------------------//
KafkaProducerCore::KafkaProducerCore()
{
	producer = nullptr;

	Init = false;
	partition = -1;
	try {
		conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	}
	catch (...) {
		conf = nullptr;
	}

	try {
		tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
	}
	catch (...) {
		tconf = nullptr;
	}
	
}
//---------------------------------------------------------------------------//
KafkaProducerCore::~KafkaProducerCore()
{
	if (conf != nullptr){
		delete conf;
	}
	
	if (tconf != nullptr) {
		delete tconf;
	}
	
	if (producer != nullptr) {
		delete producer;
	}
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaProducerCore::Initialize(std::string _brokers, std::string _topic, int32_t _partition)
{
	RetValue res;

	if (conf == nullptr) {
		res.error = err(ERR_UNHANDLED, "conf error");
		return res;
	}

	if (tconf == nullptr) {
		res.error = err(ERR_UNHANDLED, "tconf error");
		return res;
	}

	errstr.clear();
	Init = false;

	if (_brokers.empty()) {
		res.error = err(ERR_BADPARAMETR, "empty broker address");
		return res;
	}

	if (_topic.empty()) {
		res.error = err(ERR_BADPARAMETR, "empty topic");
		return res;
	}

	if (producer != nullptr) {
		delete producer;
		// Nulled in the same breath as the delete: GlobalConfDefaultInit() below can
		// still return before RdKafka::Producer::create() reassigns the member, and
		// ~KafkaProducerCore() would then delete a freed pointer. Same shape and same
		// one-line guard as in the consumer and in the admin client.
		producer = nullptr;
	}
		
	if (partition == -1) {
		partition = RdKafka::Topic::PARTITION_UA;
	}

	brokers = _brokers;
	topic = _topic;
	partition = _partition;

	res = GlobalConfDefaultInit(brokers);
	if (!res.succes) {
		return res;
	}
	
	producer = RdKafka::Producer::create(conf, errstr);
	if ((!!producer) & (errstr.empty())) {
		// librdkafka never rejects a bootstrap list at configuration time: setting
		// "metadata.broker.list" only stores the string, and rd_kafka_new() reports an
		// unparseable list through its log ("No brokers configured") instead of failing.
		// rd_kafka_brokers_add() re-runs librdkafka's own parser over the same list on
		// the live handle and returns how many brokers it yielded; re-adding the identical
		// list is idempotent, because a broker that is already RD_KAFKA_CONFIGURED is
		// counted rather than duplicated. A count of zero means librdkafka derived no
		// broker at all from the string, so the client can never connect - the same dead
		// end as an empty address, which is already rejected above. Anything that yields
		// at least one broker is left alone: that is librdkafka's verdict, and second
		// -guessing it here would reject host names librdkafka accepts.
		if (rd_kafka_brokers_add(producer->c_ptr(), brokers.c_str()) == 0) {
			delete producer;
			producer = nullptr;
			res.error = err(ERR_BADPARAMETR, "no usable broker address in \"" + BrokersForMessage(brokers) + "\"");
		}
		else {
			Init = true;
		}
	}
	else {
		res.error = err(ERR_UNHANDLED, errstr);
	}

	res.succes = Init;
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaProducerCore::ConfReset()
{
	RetValue res;

	// Nulled with the delete for the same reason as in Initialize(): the two
	// RdKafka::Conf::create() calls below can fail and return, and a member left
	// pointing at freed memory would be deleted a second time by the destructor.
	if (conf != nullptr) {
		delete conf;
		conf = nullptr;
	}

	if (tconf != nullptr) {
		delete tconf;
		tconf = nullptr;
	}

	try {
		conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	}
	catch (...) {
		conf = nullptr;
		res.error = err(ERR_UNHANDLED, "conf error");
		return res;
	}

	try {
		tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
	}
	catch (...) {
		tconf = nullptr;
		res.error = err(ERR_UNHANDLED, "tconf error");
		return res;
	}

	res = GlobalConfDefaultInit(brokers);
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaProducerCore::GlobalConfDefaultInit(std::string _brokers)
{
	RetValue res;

	if (conf == nullptr) {
		res.error = err(ERR_UNHANDLED, "conf error");
		return res;
	}

	if (tconf == nullptr) {
		res.error = err(ERR_UNHANDLED, "tconf error");
		return res;
	}

	RdKafka::Conf::ConfResult ConfSetResult;

	// Every conf->set() below is checked and the first failure returns immediately:
	// carrying on would leave a half-configured conf behind and would overwrite the
	// error that actually explains what went wrong. Identical in the consumer and in
	// the admin client.
	ConfSetResult = conf->set("default_topic_conf", tconf, errstr);
	if (ConfSetResult != RdKafka::Conf::CONF_OK) {
		res.error = err(ERR_UNHANDLED, errstr);
		return res;
	}

	ConfSetResult = conf->set("dr_cb", &dr_cb, errstr);
	if (ConfSetResult != RdKafka::Conf::CONF_OK) {
		res.error = err(ERR_UNHANDLED, errstr);
		return res;
	}

	// _brokers, not the member: the emptiness test and the value written have to
	// describe the same string.
	if (!_brokers.empty()) {
		ConfSetResult = conf->set("metadata.broker.list", _brokers, errstr);
		if (ConfSetResult != RdKafka::Conf::CONF_OK) {
			res.error = err(ERR_UNHANDLED, errstr);
			return res;
		}
	}

	res.succes = res.error.type == ERR_SUCCESS;
	return res;
}
//---------------------------------------------------------------------------//
bool KafkaProducerCore::IsInit()
{
	return Init;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaProducerCore::SetGlobalConf(std::string key, std::string value)
{
	RetValue res;

	if (conf == nullptr) {
		res.error = err(ERR_UNHANDLED, "conf error");
		return res;
	}
	
	if (key.empty()) {
		res.error = err(ERR_BADPARAMETR, "empty key");
		return res;
	}
	
	if (value.empty()) {
		res.error = err(ERR_BADPARAMETR, "empty value");
		return res;
	}

	RdKafka::Conf::ConfResult ConfSetResult;
	ConfSetResult = conf->set(key, value, errstr);
	if (ConfSetResult != RdKafka::Conf::CONF_OK) {
		res.error = err(ERR_UNHANDLED, errstr);
	}

	res.succes = res.error.type == ERR_SUCCESS;
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaProducerCore::SetTopicConf(std::string key, std::string value)
{
	RetValue res;

	if (tconf == nullptr) {
		res.error = err(ERR_UNHANDLED, "tconf error");
		return res;
	}
	
	if (key.empty()) {
		res.error = err(ERR_BADPARAMETR, "empty key");
		return res;
	}
	
	if (value.empty()) {
		res.error = err(ERR_BADPARAMETR, "empty value");
		return res;
	}

	RdKafka::Conf::ConfResult ConfSetRes = tconf->set(key, value, errstr);
	if (ConfSetRes != RdKafka::Conf::CONF_OK) {
		res.error = err(ERR_UNHANDLED, errstr);
	}

	res.succes = res.error.type == ERR_SUCCESS;
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaProducerCore::Produce(KafkaExport::DataConversion::DataConteiner* json_conteiner)
{
	RetValue res;
	if (!IsInit()){
		res.error = err(ERR_NOTINIT);
		return res;
	}
	if (!json_conteiner->IsValid()) {
		res.error = err(ERR_JSONPARSING, "invalid json");
		return res;
	}
	dr_cb.ClearRecords();

	std::string MessageKey;
	std::string MessageValue;
	RdKafka::Headers* headers = nullptr;

	for (unsigned int i = 0; i < json_conteiner->ElementsCount(); ++i) {

		MessageKey.clear();
		MessageValue.clear();
		headers = nullptr;
		try {
			if (!json_conteiner->GetKeyByIndex(i, &MessageKey)) {
				throw 0;
			}
			if (!json_conteiner->GetValueByIndex(i, &MessageValue)) {
				throw 0;
			}
			if (json_conteiner->HeadersContains(i)) {
				headers = RdKafka::Headers::create();
				if (!json_conteiner->GetHeadersByIndex(i, headers)) {
					throw 0;
				}
			}
		}
		catch (...) {
			if (headers != nullptr) {
				delete headers;
				headers = nullptr;
			}
			res.error = err(ERR_JSONPARSING, "json parsing error");
			return res;
		}

		RdKafka::ErrorCode resp =
			producer->produce(topic, partition,
				RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
				/* Value */
				(char*)MessageValue.c_str(), MessageValue.size(),
				/* Key */
				(char*)MessageKey.c_str(), MessageKey.size(),
				/* Timestamp (defaults to now) */
				0,
				/* Message headers, if any */
				headers,
				/* Per-message opaque value passed to
				* delivery report */
				nullptr);

		if (resp != RdKafka::ERR_NO_ERROR) {

			delete headers; /* Headers are automatically deleted on produce * success. */
			headers = nullptr;

			RetValue DeliveryRes = dr_cb.AddRecord(MessageKey, "Error", topic, partition, -1, -1, RdKafka::err2str(resp));
			if (!DeliveryRes.succes) {
				res.error = err(ERR_BADALLOC, "delivery report generation error");
				return res;
			}
		}
	}
	while (producer->outq_len() > 0) {
		producer->poll(1000);
	}

	res.succes = true;
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::BoolValueResult KafkaProducerCore::IsDelivered()
{
	BoolValueResult res;
	if (!IsInit()){
		res.error = err(ERR_NOTINIT);
		return res;
	}
	
	bool delivered = true;
	for (unsigned int i = 0; i < dr_cb.records.size(); ++i) {
		if (dr_cb.records[i].status != "Persisted") {
			delivered = false;
			break;
		}
	}
	res.value = delivered;
	res.succes = true;
	return res;
}
//---------------------------------------------------------------------------//
