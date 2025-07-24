#include <chrono>
#include <cstdio>
using namespace std;

class Timer {
public:
    Timer() : start_(clock::now()) {}

    void reset() {
        start_ = clock::now();
    }

    void elapsed() const {
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - start_).count();
        printf("Time taken: %lld ms\n", duration);
    }

private:
    typedef std::chrono::high_resolution_clock clock;
    std::chrono::time_point<clock> start_;
};
