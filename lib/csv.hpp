#pragma once

#include <fstream>
#include <string>
#include <iomanip>


class CsvFile {
public:
    class RecordWriter {
    public:
        RecordWriter(std::ostream& stream)
            : stream_(stream) {}

        template<class T>
        RecordWriter& operator <<(const T& value) {
            if (somethingEmitted_)
                stream_ << ',';
            stream_ << value;
            somethingEmitted_ = true;
            return *this;
        }

        template<class R, class... Args>
        RecordWriter& operator <<(R (*func) (Args...)) { // for <iomanip> and <ios>
            stream_ << func;
            return *this;
        }

        ~RecordWriter() {
            stream_ << "\n";
            stream_.flush();
        }

    private:
        std::ostream& stream_;
        bool somethingEmitted_ = false;
    };

    CsvFile(std::string path)
        : stream_(path)
    {
        stream_ << std::fixed << std::setprecision(3);
    }

    template<class T>
    RecordWriter operator <<(const T& value) {
        RecordWriter rw(stream_);
        rw << value;
        return rw;
    }

private:
    std::ofstream stream_;
};
