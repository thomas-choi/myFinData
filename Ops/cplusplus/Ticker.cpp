#include "Ticker.h"

void    Ticker::scanline(std::string    s) {
    std::string token;
    size_t      pos=0;

    while ((pos = s.find(Ticker::delimiter)) != std::string::npos) {
        token = s.substr(0, pos);
        s.erase(0, pos+1);
        std::cout << token << std::endl;
        if (token == "33")
            strncpy(&timestamp[0], token.c_str(), sizeof(timestamp)-1);
        else if (token == "3")
            last = std::stod(token);
        else if (token == "1")
            bid = std::stod(token);
        else if (token == "16")
            bidvol = std::stod(token);
        else if (token == "2")
            ask = std::stod(token);
        else if (token == "19")
            askvol = std::stod(token);
        else if (token == "17")
            volume = std::stod(token);
        else if (token == "38")
            tradeAmount = std::stod(token);
        else if (token == "37")
            high=std::stod(token);
        else if (token == "133")
            open=std::stod(token);
        else if (token == "32")
            low=std::stod(token);
        else if (token == "127")
            close=std::stod(token);
        else if (token == "31")
            pclose=std::stod(token);
        else if (token == "21")
            strncpy(&(this->name)[0], token.c_str(), sizeof(name)-1);
        else if (token == "23")
            strncpy(&currency[0], token.c_str(), sizeof(currency)-1);
        else if (token == "0")
            strncpy(&symbol[0], token.c_str(), sizeof(symbol)-1);
    }
}

void    Ticker::reset()
{
    Ticker::symbol[0] = 0;
    Ticker::timestamp[0]=0;
    Ticker::currency[0]=0;
    Ticker::name[0]=0;
    Ticker::ask=0.0;
    Ticker::askvol=0.0;
    Ticker::bid=0.0;
    Ticker::bidvol=0.0;
    Ticker::close=0.0;
    Ticker::high=0.0;
    Ticker::last=0.0;
    Ticker::low=0.0;
    Ticker::open=0.0;
    Ticker::pclose=0.0;
    Ticker::tradeAmount=0.0;
    Ticker::volume=0.0;
}
