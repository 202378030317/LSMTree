CXX = g++
CXXFLAGS = -std=c++11 -O2 -pthread -Wall
INCLUDES = -Iinclude

SRC_DIR = src
OBJ_DIR = obj

SRCS = $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(SRCS:$(SRC_DIR)/%.cpp=$(OBJ_DIR)/%.o)

TARGET = liblsm.a
TEST_TARGET = test_lsm

all: $(TARGET) $(TEST_TARGET)

$(TARGET): $(OBJS)
	ar rcs $@ $^

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(OBJ_DIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

$(TEST_TARGET): test/test_lsm.cpp $(TARGET)
	$(CXX) $(CXXFLAGS) $(INCLUDES) test/test_lsm.cpp -L. -llsm -o $(TEST_TARGET)

run: $(TEST_TARGET)
	./$(TEST_TARGET)

clean:
	rm -rf $(OBJ_DIR)
	rm -f $(TARGET)
	rm -f $(TEST_TARGET)
	rm -rf test_db*

distclean: clean
	rm -f *~
	rm -f $(SRC_DIR)/*~
	rm -f include/*~

help:
	@echo "可用命令："
	@echo "  make         - 编译库和测试程序"
	@echo "  make run     - 编译并运行测试"
	@echo "  make clean   - 清理编译产物"
	@echo "  make distclean - 彻底清理"

.PHONY: all run clean distclean help