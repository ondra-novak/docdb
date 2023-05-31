#ifndef SRC_EXAMPLE_PARSE_LINE_H_
#define SRC_EXAMPLE_PARSE_LINE_H_

#include <iostream>
#include <vector>
#include <string>
#include <sstream>

std::vector<std::string> generateWordVector(std::istream& input) {
    std::vector<std::string> wordVector;
    std::string word;
    char c;

    bool inQuotes = false;
    bool escapeNext = false;

    auto remove_quotes = [](const std::string &word) {
        if (word.back() == '"' && word.front() == '"' && word.size() > 1) {
            return word.substr(1, word.size()-2);
        }
        return word;
    };

    while (input.get(c)) {
        if (escapeNext) {
            escapeNext = false;
            if (c == 'n') {
                word += '\n';
            } else if (c == 'r') {
                word += '\r';
            } else if (c == '\\') {
                word += '\\';
            } else if (c == '\"') {
                word += '\"';
            } else if (c == '0') {
                word += '\0';
            } else if (c == 'x') {
                // Read the hex byte
                std::string hexByte;
                input >> std::setw(2) >> hexByte;
                if (hexByte.size() == 2) {
                    int byteValue;
                    std::istringstream(hexByte) >> std::hex >> byteValue;
                    word += static_cast<char>(byteValue);
                }
            } else {
                word += c;
            }
        } else if (c == '\\') {
            escapeNext = true;
        } else if (c == '"') {
            inQuotes = !inQuotes;
            word += c;
        } else if (c == ' ' && !inQuotes) {
            if (!word.empty()) {
                wordVector.push_back(remove_quotes(word));
                word.clear();
            }
        } else {
            word += c;
        }
    }

    if (!word.empty())
        wordVector.push_back(remove_quotes(word));

    return wordVector;
}





#endif /* SRC_EXAMPLE_PARSE_LINE_H_ */
