
To compile the project with Dropbox-C (https://github.com/Dwii/Dropbox-C), you need to:

1. install following libraries

sudo apt-get install libcurl4-openssl-dev
sudo apt-get install liboauth-dev
sudo apt-get install libjansson-dev

2. add "-std=c99" argument for compiler

3. add "-lcurl -loauth -ljansson" for linker