# How to use
Follow these steps:
- Clone repository.
- Make sure there is some input data in the file /input/input.txt
- Start an Erlang shell in the /src folder.
- Run the following command to compile: c(manager), c(worker), c(logger).
- Run the following command to start map-reduce on input.txt: manager:init().

The following files will be generated:
- Output can be found in /output/output.txt
- Logs can be found in /logs/log[timestamp].txt
- Intermediate files are saved to /generated folder.

Ouput follows the format "WORD - OCCURENCES".