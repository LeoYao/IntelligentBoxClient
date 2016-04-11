/*
 * common_utils.hpp
 *
 *  Created on: Feb 20, 2016
 *      Author: hadoop
 */

#ifndef COMMON_UTILS_HPP_
#define COMMON_UTILS_HPP_

#include <string.h>
#include <stdbool.h>
#include <stdarg.h>
#include <string.h>


#define EXPAND_OK 0

void delay(unsigned int mseconds);
char* concat_string(int n, ...);
int getLastSlashPosition(const char* path);
int expand_mem(void** ptr, int size);
char* copy_text(const char* tmp);

#endif /* COMMON_UTILS_HPP_ */
