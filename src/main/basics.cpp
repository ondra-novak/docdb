#include <iostream>
#include "../docdb/types.h"

int main(int, char **) {

    std::string s;
    docdb::SmallBlob b("hello");
    docdb::structured::build_string(s, 10,12.625,nullptr,b,"world",true,false);
    
    docdb::structured::to_strings(s, [](const std::string_view &s){
        std::cout << s << std::endl;
    });
    
    for (char c : s) {
        std::cout << std::hex << int(static_cast<unsigned char>(c)) << " " << c << std::endl; 
    }
    
    
    
}
