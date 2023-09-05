CC=gcc-11
CPP=g++-11
CXX=g++-11
LD=g++-11
AR=ar

cxxflags = $(CPPFLAGS) $(CXXFLAGS) -I./cplusplus \
	-g -Wall -Wextra -Werror -std=c++20 -pthread -fPIC
ldflags = $(LDFLAGS) -static
arflags = rc
LDLIBS = -L./cplusplus -lgrinder -pthread -lboost_program_options

sources = \
	cplusplus/UNQueue.cpp \
	cplusplus/mmapStorage.cpp \
	cplusplus/Ticker.cpp \
	cplusplus/MarketImage.cpp

objects = $(sources:.cpp=.o)

$(info V is "$(V)")

ifeq ($(V),1)
	VCXX   = $(CXX) -c
	VCXXLD = $(CXX)
	VDEPS  = $(CXX) -MM
else
	VCXX   = @echo "  COMPILE $@" && $(CXX) -c
	VCXXLD = @echo "  LINK $@" && $(CXX)
	VDEPS  = @echo "  DEPENDS $@" && $(CXX) -MM
endif

all: DDSClient testReader

testReader: cplusplus/testReader.cpp $(objects)
	$(VCXXLD) $(strip $(cxxflags) -o $@ cplusplus/testReader.cpp $(objects) $(ldflags) $(LDLIBS))

DDSClient: cplusplus/DDSClient.cpp $(objects)
	$(VCXXLD) $(strip $(cxxflags) -o $@ cplusplus/DDSClient.cpp $(objects) $(ldflags) $(LDLIBS))

.cpp.o:
	$(VCXX) $(strip $(cxxflags) -o $@ $<)

-include Makefile.deps
make.fin.deps: $(sources) cplusplus/DDSClient.cpp
	$(VDEPS) $(strip $(cxxflags) $(sources) cplusplus/DDSClient.cpp > $@)

clean:
	$(RM) *.o $(objects) DDSCopyClient

.PHONY: all clean
