// Copyright (C) 2021-2022 Joel Rosdahl and other contributors
//
// See doc/AUTHORS.adoc for a complete list of contributors.
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 3 of the License, or (at your option)
// any later version.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
// more details.
//
// You should have received a copy of the GNU General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 51
// Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

#include "MongoStorage.hpp"

#include <Digest.hpp>
#include <Logging.hpp>
#include <core/exceptions.hpp>
#include <fmtmacros.hpp>
#include <util/expected.hpp>
#include <util/string.hpp>
#include <sstream>
#include <cstdarg>
#include <map>
#include <memory>
#include <cstdlib>
#include <string>
#include <cstdint>
#include <iostream>
#include <vector>
#include <fstream>
#include <sys/stat.h>
// Ignore "ISO C++ forbids flexible array member ‘buf’" warning from -Wpedantic.
#ifdef __GNUC__
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wpedantic"
#endif
#ifdef _MSC_VER
#  pragma warning(push)
#  pragma warning(disable : 4200)
#endif

#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/instance.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/array.hpp>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/secretsmanager/SecretsManagerClient.h>
#include <aws/secretsmanager/model/CreateSecretRequest.h>

#include <boost/interprocess/streams/bufferstream.hpp>

#ifdef _MSC_VER
#  pragma warning(pop)
#endif
#ifdef __GNUC__
#  pragma GCC diagnostic pop
#endif





using bsoncxx::builder::stream::close_array;
using bsoncxx::builder::stream::close_document;
using bsoncxx::builder::stream::document;
using bsoncxx::builder::stream::finalize;
using bsoncxx::builder::stream::open_array;
using bsoncxx::builder::stream::open_document;

namespace storage::remote {

namespace {

//using RedisContext = std::unique_ptr<redisContext, decltype(&redisFree)>;
//using RedisReply = std::unique_ptr<redisReply, decltype(&freeReplyObject)>;

const uint32_t DEFAULT_PORT = 6379;

class MongoStorageBackend : public RemoteStorage::Backend
{
public:
  MongoStorageBackend(const RemoteStorage::Backend::Params& params);

  nonstd::expected<std::optional<util::Bytes>, Failure>
  get(const Digest& key) override;

  nonstd::expected<bool, Failure> put(const Digest& key,
                                      nonstd::span<const uint8_t> value,
                                      bool only_if_missing) override;

  nonstd::expected<bool, Failure> remove(const Digest& key) override;

private:
  const std::string m_prefix;
  mongocxx::client conn;
  mongocxx::database db;
  mongocxx::collection coll;

  std::string get_key_string(const Digest& digest) const;
};


MongoStorageBackend::MongoStorageBackend(const Params& params)
  : m_prefix("ccache") // TODO: attribute
    //m_context(nullptr, redisFree)
{

  mongocxx::instance inst{};
  LOG("Mongo URL {}", params.url.str());
  const auto uri = mongocxx::uri{params.url.str()};
  mongocxx::options::client client_options;
  auto api = mongocxx::options::server_api{mongocxx::options::server_api::version::k_version_1};
  client_options.server_api_opts (api);
  conn = mongocxx::client{uri, client_options};
  db = conn["ccache"];
  coll = db["devprod"];

  // const auto& url = params.url;
  // ASSERT(url.scheme() == "redis" || url.scheme() == "redis+unix");
  // if (url.scheme() == "redis+unix" && !params.url.host().empty()
  //     && params.url.host() != "localhost") {
  //   throw core::Fatal(
  //     FMT("invalid file path \"{}\": specifying a host other than localhost is"
  //         " not supported",
  //         params.url.str(),
  //         params.url.host()));
  // }

  // auto connect_timeout = k_default_connect_timeout;
  // auto operation_timeout = k_default_operation_timeout;

  // for (const auto& attr : params.attributes) {
  //   if (attr.key == "connect-timeout") {
  //     connect_timeout = parse_timeout_attribute(attr.value);
  //   } else if (attr.key == "operation-timeout") {
  //     operation_timeout = parse_timeout_attribute(attr.value);
  //   } else if (!is_framework_attribute(attr.key)) {
  //     LOG("Unknown attribute: {}", attr.key);
  //   }
  // }

  // connect(url, connect_timeout.count(), operation_timeout.count());
  // authenticate(url);
  // select_database(url);
}

// inline bool
// is_error(int err)
// {
//   return err != REDIS_OK;
// }

// inline bool
// is_timeout(int err)
// {
// #ifdef REDIS_ERR_TIMEOUT
//   // Only returned for hiredis version 1.0.0 and above
//   return err == REDIS_ERR_TIMEOUT;
// #else
//   (void)err;
//   return false;
// #endif
// }

nonstd::expected<std::optional<util::Bytes>, RemoteStorage::Backend::Failure>
MongoStorageBackend::get(const Digest& key)
{
  const auto key_string = get_key_string(key);
  LOG("Mongo GET {}", key_string);

  bsoncxx::stdx::optional<bsoncxx::document::value> maybe_result =
    coll.find_one(document{} << "key" << key_string.c_str() << finalize);
  if (maybe_result){
  LOG("Mongo finished {}", key_string);
  Aws::SDKOptions options;
  Aws::InitAPI(options);
  
  Aws::Client::ClientConfiguration clientConfig;

  // Optional: Set to the AWS Region in which the bucket was created (overrides config file).
  clientConfig.region = "us-east-2";
  //Aws::SecretsManager::SecretsManagerClient sm_client(clientConfig);

  // Aws::String secretName = std::getenv("AWS_ACCESS_KEY_ID");
  // Aws::String secretString = std::getenv("AWS_SECRET_ACCESS_KEY");
  // Aws::SecretsManager::Model::CreateSecretRequest secrectRequest;
  // secrectRequest.SetName(secretName);
  // secrectRequest.SetSecretString(secretString);

  // auto createSecretOutcome = sm_client.CreateSecret(secrectRequest);
  // if(createSecretOutcome.IsSuccess()){
  //         std::cout << "Create secret with name: " << createSecretOutcome.GetResult().GetName() << std::endl;
  // }else{
  //         std::cout << "Failed with Error: " << createSecretOutcome.GetError() << std::endl;
  // }

  Aws::S3::S3Client client(clientConfig);

  //TODO(user): Change bucket_name to the name of a bucket in your account.
  const Aws::String bucket_name = "daniel.moody";
  //TODO(user): Create a file called "my-file.txt" in the local folder where your executables are built to.
  const Aws::String object_name = key_string.c_str();
  LOG("S3 setup {}", key_string);
  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(bucket_name);
  request.SetKey(object_name);
  

  Aws::S3::Model::GetObjectOutcome outcome =
          client.GetObject(request);

  if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error &err = outcome.GetError();
      std::cerr << "Error: GetObject: " <<
                err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
  }
  // else {
  //     std::cout << "Successfully retrieved '" << object_name << "' from '"
  //               << bucket_name << "'." << std::endl;
  // }
  LOG("S3 recieved {}", key_string);
  std::vector<uint8_t> data;
  std::for_each(std::istreambuf_iterator<char>(outcome.GetResult().GetBody()),
              std::istreambuf_iterator<char>(),
              [&data](const char c){
                  data.push_back((const unsigned char)c);
              });
  auto resutl = util::Bytes((const void*)&data[0], data.size());
  LOG("S3 finished {}", key_string);
  return resutl;
  }
  else{
    return std::nullopt;
  }

  // const auto reply = redis_command("GET %s", key_string.c_str());
  // if (!reply) {
  //   return nonstd::make_unexpected(reply.error());
  // } else if ((*reply)->type == REDIS_REPLY_STRING) {
  //   return util::Bytes((*reply)->str, (*reply)->len);
  // } else if ((*reply)->type == REDIS_REPLY_NIL) {
  //   return std::nullopt;
  // } else {
  //   LOG("Unknown reply type: {}", (*reply)->type);
  //   return nonstd::make_unexpected(Failure::error);
  // }

}

nonstd::expected<bool, RemoteStorage::Backend::Failure>
MongoStorageBackend::put(const Digest& key,
                         nonstd::span<const uint8_t> value,
                         bool only_if_missing)
{
  const auto key_string = get_key_string(key);

  
    LOG("Redis EXISTS {}", key_string);


    auto builder = bsoncxx::builder::stream::document{};
    bsoncxx::document::value doc_value = builder
      << "key" << key_string.c_str()
      // << "type" << "database"
      // << "count" << 1
      // << "versions" << bsoncxx::builder::stream::open_array
      //   << "v3.2" << "v3.0" << "v2.6"
      // << close_array
      // << "info" << bsoncxx::builder::stream::open_document
      //   << "x" << 203
      //   << "y" << 102
      // << bsoncxx::builder::stream::close_document
      << bsoncxx::builder::stream::finalize;
    bsoncxx::document::view view = doc_value.view();
    bsoncxx::stdx::optional<mongocxx::result::insert_one> result = coll.insert_one(view);

    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        //TODO(user): Change bucket_name to the name of a bucket in your account.
        const Aws::String bucket_name = "daniel.moody";
        //TODO(user): Create a file called "my-file.txt" in the local folder where your executables are built to.
        const Aws::String object_name = key_string.c_str();

        Aws::Client::ClientConfiguration clientConfig;
        
        // Optional: Set to the AWS Region in which the bucket was created (overrides config file).
        clientConfig.region = "us-east-2";
        // Aws::SecretsManager::SecretsManagerClient sm_client(clientConfig);

        // Aws::String secretName = std::getenv("AWS_ACCESS_KEY_ID");
        // Aws::String secretString = std::getenv("AWS_SECRET_ACCESS_KEY");
        // Aws::SecretsManager::Model::CreateSecretRequest secrectRequest;
        // secrectRequest.SetName(secretName);
        // secrectRequest.SetSecretString(secretString);

        // auto createSecretOutcome = sm_client.CreateSecret(secrectRequest);
        // if(createSecretOutcome.IsSuccess()){
        //         std::cout << "Create secret with name: " << createSecretOutcome.GetResult().GetName() << std::endl;
        // }else{
        //         std::cout << "Failed with Error: " << createSecretOutcome.GetError() << std::endl;
        // }
        Aws::S3::S3Client s3_client(clientConfig);
     
        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(bucket_name);
        //We are using the name of the file as the key for the object in the bucket.
        //However, this is just a string and can be set according to your retrieval needs.
        request.SetKey(object_name);
        // make an imput stream from my_vec
        std::shared_ptr<Aws::IOStream> inputData;
        std::shared_ptr<Aws::IOStream> body =  std::shared_ptr<Aws::IOStream>(new boost::interprocess::bufferstream((char*)&value[0], value.size()));

        // std::shared_ptr<Aws::IOStream> inputData =
        //         Aws::MakeShared<Aws::FStream>("ccacheTag",
        //                                       fileName .c_str(),
        //                                       std::ios_base::in | std::ios_base::binary);

        // if (!*inputData) {
        //     std::cerr << "Error unable to read file " << fileName << std::endl;
        //     return false;
        // }

        request.SetBody(body);
        
        Aws::S3::Model::PutObjectOutcome outcome =
                s3_client.PutObject(request);

        if (!outcome.IsSuccess()) {
            std::cerr << "Error: PutObject: " <<
                      outcome.GetError().GetMessage() << std::endl;
        }
        // else {
        //     std::cout << "Added object '" << object_name << "' to bucket '"
        //               << bucket_name << "'.";
        // }
    }
    Aws::ShutdownAPI(options);


    // const auto reply = redis_command("EXISTS %s", key_string.c_str());
    // if (!reply) {
    //   return nonstd::make_unexpected(reply.error());
    // } else if ((*reply)->type != REDIS_REPLY_INTEGER) {
    //   LOG("Unknown reply type: {}", (*reply)->type);
    // } else if ((*reply)->integer > 0) {
    //   LOG("Entry {} already in Redis", key_string);
    //   return false;
    // }
  

  // LOG("Redis SET {} [{} bytes]", key_string, value.size());
  // const auto reply =
  //   redis_command("SET %s %b", key_string.c_str(), value.data(), value.size());
  // if (!reply) {
  //   return nonstd::make_unexpected(reply.error());
  // } else if ((*reply)->type == REDIS_REPLY_STATUS) {
  //   return true;
  // } else {
  //   LOG("Unknown reply type: {}", (*reply)->type);
  //   return nonstd::make_unexpected(Failure::error);
  // }
  return true;
}

nonstd::expected<bool, RemoteStorage::Backend::Failure>
MongoStorageBackend::remove(const Digest& key)
{
  const auto key_string = get_key_string(key);
  LOG("Redis DEL {}", key_string);

  coll.delete_one(document{} << "key" << key_string.c_str() << finalize);
  Aws::SDKOptions options;
  Aws::InitAPI(options);
  
  Aws::Client::ClientConfiguration clientConfig;

  // Optional: Set to the AWS Region in which the bucket was created (overrides config file).
  clientConfig.region = "us-east-2";
  // Aws::SecretsManager::SecretsManagerClient sm_client(clientConfig);

  // Aws::String secretName = std::getenv("AWS_ACCESS_KEY_ID");
  // Aws::String secretString = std::getenv("AWS_SECRET_ACCESS_KEY");
  // Aws::SecretsManager::Model::CreateSecretRequest secrectRequest;
  // secrectRequest.SetName(secretName);
  // secrectRequest.SetSecretString(secretString);

  // auto createSecretOutcome = sm_client.CreateSecret(secrectRequest);
  // if(createSecretOutcome.IsSuccess()){
  //         std::cout << "Create secret with name: " << createSecretOutcome.GetResult().GetName() << std::endl;
  // }else{
  //         std::cout << "Failed with Error: " << createSecretOutcome.GetError() << std::endl;
  // }

  Aws::S3::S3Client client(clientConfig);

  //TODO(user): Change bucket_name to the name of a bucket in your account.
  const Aws::String bucket_name = "daniel.moody";
  //TODO(user): Create a file called "my-file.txt" in the local folder where your executables are built to.
  const Aws::String object_name = key_string.c_str();

  Aws::S3::Model::DeleteObjectRequest request;

  request.WithKey(object_name)
          .WithBucket(bucket_name);

  Aws::S3::Model::DeleteObjectOutcome outcome =
          client.DeleteObject(request);

  if (!outcome.IsSuccess()) {
      auto err = outcome.GetError();
      std::cerr << "Error: DeleteObject: " <<
                err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
  }
  // else {
  //     std::cout << "Successfully deleted the object." << std::endl;
  // }

  return outcome.IsSuccess();

  // const auto reply = redis_command("DEL %s", key_string.c_str());
  // if (!reply) {
  //   return nonstd::make_unexpected(reply.error());
  // } else if ((*reply)->type == REDIS_REPLY_INTEGER) {
  //   return (*reply)->integer > 0;
  // } else {
  //   LOG("Unknown reply type: {}", (*reply)->type);
  //   return nonstd::make_unexpected(Failure::error);
  // }
}


std::string
MongoStorageBackend::get_key_string(const Digest& digest) const
{
  return FMT("{}:{}", m_prefix, digest.to_string());
}

} // namespace

std::unique_ptr<RemoteStorage::Backend>
MongoStorage::create_backend(const Backend::Params& params) const
{
  return std::make_unique<MongoStorageBackend>(params);
}

} // namespace storage::remote
