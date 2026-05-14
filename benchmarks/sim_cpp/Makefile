# SPDX-License-Identifier: Apache-2.0

CXX      := g++
CXXFLAGS := -std=c++17 -O3 -Wall -Wextra -Wno-unused-parameter -pthread
LDFLAGS  := -pthread

SRC      := main.cpp
HDRS     := cluster.hpp workload.hpp schedulers.hpp simulator.hpp report.hpp
BIN      := sim

.PHONY: all clean

all: $(BIN)

$(BIN): $(SRC) $(HDRS)
	$(CXX) $(CXXFLAGS) -o $@ $(SRC) $(LDFLAGS)

clean:
	rm -f $(BIN)
