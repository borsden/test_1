#pragma once

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <parquet/schema.h>
#include <parquet/stream_writer.h>

namespace arrow {
namespace io {
class FileOutputStream;
}  // namespace io
}  // namespace arrow

namespace b3::sbe {

class ParquetStream {
  public:
    ParquetStream(const std::filesystem::path &path,
                  std::shared_ptr<parquet::schema::GroupNode> schema);
    ~ParquetStream();

    parquet::StreamWriter &writer() { return *writer_; }
    void Close();
    int64_t rows() const { return rows_; }
    void IncrementRow() { ++rows_; }

  private:
    std::shared_ptr<arrow::io::FileOutputStream> sink_;
    std::unique_ptr<parquet::StreamWriter> writer_;
    int64_t rows_{0};
};

template <typename Row>
class TableStreamWriter {
  public:
    using WriteFunc = void (*)(parquet::StreamWriter &, const Row &);
    TableStreamWriter(const std::filesystem::path &output_dir, const std::string &table_name,
                      std::shared_ptr<parquet::schema::GroupNode> schema, WriteFunc func)
        : stream_(std::make_unique<ParquetStream>(output_dir / (table_name + ".parquet"),
                                                  std::move(schema))),
          func_(func)
    {
    }

    void Append(const Row &row)
    {
        func_(stream_->writer(), row);
        stream_->writer() << parquet::EndRow;
        stream_->IncrementRow();
    }

    void Close() { stream_->Close(); }

    int64_t rows() const { return stream_->rows(); }

  private:
    std::unique_ptr<ParquetStream> stream_;
    WriteFunc func_;
};

std::shared_ptr<parquet::schema::GroupNode> MakeStringSchema(
    const std::string &table_name, const std::vector<parquet::schema::NodePtr> &fields);

parquet::schema::NodePtr MakeStringColumn(const std::string &name,
                                          parquet::Repetition::type repetition =
                                              parquet::Repetition::REQUIRED);
parquet::schema::NodePtr MakeBinaryColumn(const std::string &name,
                                          parquet::Repetition::type repetition =
                                              parquet::Repetition::REQUIRED);
parquet::schema::NodePtr MakeInt32Column(const std::string &name,
                                         parquet::Repetition::type repetition =
                                             parquet::Repetition::REQUIRED);
parquet::schema::NodePtr MakeInt64Column(const std::string &name,
                                         parquet::Repetition::type repetition =
                                             parquet::Repetition::REQUIRED);
parquet::schema::NodePtr MakeDoubleColumn(const std::string &name,
                                          parquet::Repetition::type repetition =
                                              parquet::Repetition::REQUIRED);
parquet::schema::NodePtr MakeBoolColumn(const std::string &name,
                                        parquet::Repetition::type repetition =
                                            parquet::Repetition::REQUIRED);

}  // namespace b3::sbe
