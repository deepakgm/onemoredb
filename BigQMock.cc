#include "gmock/gmock.h"

class MockBigQ : public BigQ {
    MOCK_METHOD(void, phaseOne, (), (const, override));
};