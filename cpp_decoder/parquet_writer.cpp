#include "parquet_writer.h"

#include <arrow/io/file.h>
#include <arrow/status.h>
#include <parquet/exception.h>
#include <parquet/schema.h>

#include <stdexcept>

namespace b3::sbe {
namespace {

std::shared_ptr<parquet::WriterProperties> MakeWriterProperties()
{
    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::SNAPPY);
    return builder.build();
}

}  // namespace

ParquetStream::ParquetStream(const std::filesystem::path &path,
                             std::shared_ptr<parquet::schema::GroupNode> schema)
{
    auto outfile_result = arrow::io::FileOutputStream::Open(path.string());
    if (!outfile_result.ok()) {
        throw std::runtime_error(std::string("Unable to open parquet output ") + path.string() +
                                 ": " + outfile_result.status().ToString());
    }
    sink_ = *std::move(outfile_result);

    auto writer = parquet::ParquetFileWriter::Open(sink_, schema, MakeWriterProperties());
    writer_ = std::make_unique<parquet::StreamWriter>(std::move(writer));
}

ParquetStream::~ParquetStream()
{
    try {
        Close();
    } catch (...) {
    }
}

void ParquetStream::Close()
{
    if (writer_) {
        writer_.reset();
    }
    if (sink_) {
        auto status = sink_->Close();
        if (!status.ok()) {
            throw std::runtime_error(std::string("Unable to close parquet sink: ") + status.ToString());
        }
        sink_.reset();
    }
}

std::shared_ptr<parquet::schema::GroupNode> MakeStringSchema(
    const std::string &table_name, const std::vector<parquet::schema::NodePtr> &fields)
{
    return std::static_pointer_cast<parquet::schema::GroupNode>(
        parquet::schema::GroupNode::Make(table_name, parquet::Repetition::REQUIRED, fields));
}

parquet::schema::NodePtr MakeStringColumn(const std::string &name,
                                          parquet::Repetition::type repetition)
{
    return parquet::schema::PrimitiveNode::Make(name, repetition, parquet::Type::BYTE_ARRAY,
                                                parquet::ConvertedType::UTF8);
}

parquet::schema::NodePtr MakeBinaryColumn(const std::string &name,
                                          parquet::Repetition::type repetition)
{
    return parquet::schema::PrimitiveNode::Make(name, repetition, parquet::Type::BYTE_ARRAY,
                                                parquet::ConvertedType::NONE);
}

parquet::schema::NodePtr MakeInt32Column(const std::string &name,
                                         parquet::Repetition::type repetition)
{
    return parquet::schema::PrimitiveNode::Make(name, repetition, parquet::Type::INT32,
                                                parquet::ConvertedType::NONE);
}

parquet::schema::NodePtr MakeInt64Column(const std::string &name,
                                         parquet::Repetition::type repetition)
{
    return parquet::schema::PrimitiveNode::Make(name, repetition, parquet::Type::INT64,
                                                parquet::ConvertedType::NONE);
}

parquet::schema::NodePtr MakeDoubleColumn(const std::string &name,
                                          parquet::Repetition::type repetition)
{
    return parquet::schema::PrimitiveNode::Make(name, repetition, parquet::Type::DOUBLE,
                                                parquet::ConvertedType::NONE);
}

parquet::schema::NodePtr MakeBoolColumn(const std::string &name,
                                        parquet::Repetition::type repetition)
{
    return parquet::schema::PrimitiveNode::Make(name, repetition, parquet::Type::BOOLEAN,
                                                parquet::ConvertedType::NONE);
}

}  // namespace b3::sbe
