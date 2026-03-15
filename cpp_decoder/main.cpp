#include "decoder.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace fs = std::filesystem;

struct CliOptions {
    std::vector<fs::path> inputs;
    fs::path output;
    std::vector<std::string> tables;
    std::uint64_t max_packets{0};
    bool validate_schema{true};
};

void PrintUsage()
{
    std::cout << "Usage: b3sbe_decode --input <path> [--input <path> ...] --output <dir>"
                 " [--tables t1,t2] [--max-packets N] [--no-validate]\n";
}

std::vector<std::string> Split(const std::string &value, char delim)
{
    std::vector<std::string> out;
    std::stringstream ss(value);
    std::string token;
    while (std::getline(ss, token, delim)) {
        if (!token.empty()) {
            out.push_back(token);
        }
    }
    return out;
}

std::vector<fs::path> CollectInputs(const std::vector<fs::path> &inputs)
{
    std::vector<fs::path> files;
    for (const auto &path : inputs) {
        if (!fs::exists(path)) {
            throw std::runtime_error("Input path does not exist: " + path.string());
        }
        if (fs::is_regular_file(path)) {
            if (path.extension() == ".pcap" || path.extension() == ".pcapng") {
                files.push_back(path);
            }
            continue;
        }
        if (fs::is_directory(path)) {
            for (auto &entry : fs::directory_iterator(path)) {
                if (!entry.is_regular_file()) {
                    continue;
                }
                auto ext = entry.path().extension().string();
                std::transform(ext.begin(), ext.end(), ext.begin(), [](unsigned char c) {
                    return static_cast<char>(std::tolower(c));
                });
                if (ext == ".pcap" || ext == ".pcapng") {
                    files.push_back(entry.path());
                }
            }
            continue;
        }
        throw std::runtime_error("Unsupported input type: " + path.string());
    }
    std::sort(files.begin(), files.end());
    files.erase(std::unique(files.begin(), files.end()), files.end());
    if (files.empty()) {
        throw std::runtime_error("No pcap files found under provided inputs");
    }
    return files;
}

CliOptions ParseArgs(int argc, char **argv)
{
    CliOptions opts;
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        if (arg == "--input") {
            if (i + 1 >= argc) {
                throw std::runtime_error("--input requires a value");
            }
            opts.inputs.emplace_back(argv[++i]);
        } else if (arg == "--output") {
            if (i + 1 >= argc) {
                throw std::runtime_error("--output requires a value");
            }
            opts.output = fs::path(argv[++i]);
        } else if (arg == "--tables") {
            if (i + 1 >= argc) {
                throw std::runtime_error("--tables requires a value");
            }
            opts.tables = Split(argv[++i], ',');
        } else if (arg == "--max-packets") {
            if (i + 1 >= argc) {
                throw std::runtime_error("--max-packets requires a value");
            }
            opts.max_packets = std::stoull(argv[++i]);
        } else if (arg == "--no-validate") {
            opts.validate_schema = false;
        } else if (arg == "--help" || arg == "-h") {
            PrintUsage();
            std::exit(0);
        } else {
            throw std::runtime_error("Unknown argument: " + arg);
        }
    }
    if (opts.inputs.empty()) {
        throw std::runtime_error("At least one --input must be provided");
    }
    if (opts.output.empty()) {
        throw std::runtime_error("--output directory is required");
    }
    return opts;
}

int main(int argc, char **argv)
{
    try {
        auto cli = ParseArgs(argc, argv);
        auto files = CollectInputs(cli.inputs);
        b3::sbe::DecodeOptions decode_opts{};
        decode_opts.validate_schema = cli.validate_schema;
        decode_opts.max_packets_per_file = cli.max_packets;
        b3::sbe::NativeDecoder decoder(decode_opts);
        b3::sbe::OutputOptions output{};
        output.output_dir = cli.output;
        output.tables = cli.tables;
        auto counts = decoder.parse_files(files, output);
        std::cout << "Decoded " << files.size() << " file(s) into " << cli.output << "\n";
        for (const auto &kv : counts) {
            std::cout << "  " << kv.first << ": " << kv.second << " rows\n";
        }
        return 0;
    } catch (const std::exception &ex) {
        std::cerr << "Error: " << ex.what() << "\n";
        PrintUsage();
        return 1;
    }
}
