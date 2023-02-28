#include <gtest/gtest.h>

#include "implementations/DSU_Usual.h"
#include "implementations/DSU_Adaptive.h"
#include "implementations/DSU_AdaptiveSmart.h"
#include "implementations/DSU_AdaptiveLocks.h"
#include "implementations/DSU_LazyUnion.h"
#include "implementations/DSU_ParallelUnions.h"

#include "lib/numa.hpp"

#include <barrier>
#include <memory>


template <class DSU>
class DSUTest : public ::testing::Test {
public:
    NUMAContext Ctx_{2};

    std::unique_ptr<DSU> MakeDSU(int N) {
        return std::make_unique<DSU>(&Ctx_, N);
    }
};

using Dsus = ::testing::Types<DSU_Adaptive<true, false>, DSU_Adaptive<true, true>, DSU_AdaptiveLocks<true>, DSU_LazyUnions<true>, DSU_ParallelUnions<true>,
        DSU_Adaptive<false, false>, DSU_Adaptive<false, true>, DSU_AdaptiveLocks<false>, DSU_LazyUnions<false>, DSU_ParallelUnions<false>,
        DSU_AdaptiveSmart<false>, DSU_AdaptiveSmart<true>>;
TYPED_TEST_SUITE(DSUTest, Dsus);

TYPED_TEST(DSUTest, Simple) {
    this->Ctx_.SetupForTests(4, 2);
    auto dsu = this->MakeDSU(5);
    this->Ctx_.StartNThreads([&]{
        dsu->Union(0, 1);
        dsu->Union(1, 2);
        dsu->Union(4, 0);
        EXPECT_TRUE(dsu->SameSet(0, 1));
        EXPECT_TRUE(dsu->SameSet(1, 2));
        EXPECT_TRUE(dsu->SameSet(2, 4));
        EXPECT_FALSE(dsu->SameSet(3, 0));
        EXPECT_FALSE(dsu->SameSet(3, 1));
        EXPECT_FALSE(dsu->SameSet(3, 2));
        EXPECT_FALSE(dsu->SameSet(3, 4));
    }, 4);
    this->Ctx_.Join();
}

TYPED_TEST(DSUTest, SimpleConcurrent) {
    this->Ctx_.SetupForTests(4, 2);
    auto dsu = this->MakeDSU(5);
    std::barrier barrier(4);
    this->Ctx_.StartNThreads([&]{
        if (NUMAContext::CurrentThreadNode() == 0) {
            dsu->Union(0, 1);
            dsu->Union(1, 2);
            dsu->Union(4, 0);
            barrier.arrive_and_wait();
        } else {
            barrier.arrive_and_wait();
            EXPECT_TRUE(dsu->SameSet(0, 1));
            EXPECT_TRUE(dsu->SameSet(1, 2));
            EXPECT_TRUE(dsu->SameSet(2, 4));
            EXPECT_FALSE(dsu->SameSet(3, 0));
            EXPECT_FALSE(dsu->SameSet(3, 1));
            EXPECT_FALSE(dsu->SameSet(3, 2));
            EXPECT_FALSE(dsu->SameSet(3, 4));
        }
    }, 4);
    this->Ctx_.Join();
}
