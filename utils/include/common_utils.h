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

void delay(unsigned int mseconds);
char* concat_string(int n, ...);
int getLastSlashPosition(const char* path);


#endif /* COMMON_UTILS_HPP_ */
