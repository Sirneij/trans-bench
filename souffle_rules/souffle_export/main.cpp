#include <souffle/SouffleInterface.h>
#include <iostream>
#include <string>
#include <chrono>
#include <sys/resource.h>

// Function to get CPU time usage
void getCPUTimes(long double &userTime, long double &sysTime)
{
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    userTime = (long double)usage.ru_utime.tv_sec + (long double)usage.ru_utime.tv_usec / 1000000.0;
    sysTime = (long double)usage.ru_stime.tv_sec + (long double)usage.ru_stime.tv_usec / 1000000.0;
}

int main(int argc, char *argv[])
{
    using namespace std::chrono;

    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0] << " <facts_folder>" << std::endl;
        return 1;
    }

    const std::string factsFolder = argv[1];
    std::cout << "Facts folder: " << factsFolder << std::endl;

    // Initialize Souffle program
    long double startUserInstance, startSysInstance, endUserInstance, endSysInstance;
    auto start_instance = high_resolution_clock::now();
    getCPUTimes(startUserInstance, startSysInstance);
    souffle::SouffleProgram *prog = souffle::ProgramFactory::newInstance("souffle_generated");
    getCPUTimes(endUserInstance, endSysInstance);
    auto end_instance = high_resolution_clock::now();
    if (prog == nullptr)
    {
        std::cerr << "Could not create Souffle program instance." << std::endl;
        return 1;
    }

    std::cout << "Program created." << std::endl;

    // Load facts, run program, and print results
    long double startUserLoadFacts, startSysLoadFacts, endUserLoadFacts, endSysLoadFacts;
    auto start_load = high_resolution_clock::now();
    getCPUTimes(startUserLoadFacts, startSysLoadFacts);
    prog->loadAll(factsFolder);
    getCPUTimes(endUserLoadFacts, endSysLoadFacts);
    auto end_load = high_resolution_clock::now();

    long double startUserRun, startSysRun, endUserRun, endSysRun;
    auto start_run = high_resolution_clock::now();
    getCPUTimes(startUserRun, startSysRun);
    prog->run();
    getCPUTimes(endUserRun, endSysRun);
    auto end_run = high_resolution_clock::now();

    long double startUserPrint, startSysPrint, endUserPrint, endSysPrint;
    auto start_print = high_resolution_clock::now();
    getCPUTimes(startUserPrint, startSysPrint);
    prog->printAll();
    getCPUTimes(endUserPrint, endSysPrint);
    auto end_print = high_resolution_clock::now();

    // Calculate CPU times
    long double instanceCPUTime = (endUserInstance - startUserInstance) + (endSysInstance - startSysInstance);
    long double loadFactsCPUTime = (endUserLoadFacts - startUserLoadFacts) + (endSysLoadFacts - startSysLoadFacts);
    long double runCPUTime = (endUserRun - startUserRun) + (endSysRun - startSysRun);
    long double printCPUTime = (endUserPrint - startUserPrint) + (endSysPrint - startSysPrint);

    auto instance_duration = duration_cast<duration<double>>(end_instance - start_instance);
    auto load_duration = duration_cast<duration<double>>(end_load - start_load);
    auto run_duration = duration_cast<duration<double>>(end_run - start_run);
    auto print_duration = duration_cast<duration<double>>(end_print - start_print);

    // Print times
    std::cout << "Instance time: " << instance_duration.count() << " seconds\n";
    std::cout << "InstanceCPU time: " << instanceCPUTime << " seconds\n";
    std::cout << "LoadingFacts time: " << load_duration.count() << " seconds\n";
    std::cout << "LoadingFactsCPU time: " << loadFactsCPUTime << " seconds\n";
    std::cout << "Query time: " << run_duration.count() << " seconds\n";
    std::cout << "QueryCPU time: " << runCPUTime << " seconds\n";
    std::cout << "Writing time: " << print_duration.count() << " seconds\n";
    std::cout << "WritingCPU time: " << printCPUTime << " seconds\n";

    // Cleanup
    delete prog;
    return 0;
}