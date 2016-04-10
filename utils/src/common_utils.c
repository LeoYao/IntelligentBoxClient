/*
 * common_utils.cpp
 *
 *  Created on: Feb 20, 2016
 *      Author: hadoop
 */

#include <common_utils.h>
#include <sys/time.h>

//Delay method
void delay(unsigned int mseconds)
{
	struct timespec sleep_time;
	sleep_time.tv_sec = 0;
	sleep_time.tv_nsec = (__syscall_slong_t)mseconds * 1000000 ;//to nanosecond

	nanosleep(&sleep_time, NULL);
}

int getLastSlashPosition(const char* path)
{
	int len = strlen(path);

	for (int i = len -1; i >= 0; i--)
	{
		if (path[i] == '/')
		{
			return i + 1;
		}
	}

	return 1;
}


char* concat_string(const int n, ...){

	va_list ap;
	va_start(ap, n);
	char* s = NULL;
	int end = 0;
	int len = 0;

	if (n < 1){
		s = NULL;
	} else if (n == 1){
		char* tmp = va_arg(ap,char*);
		len = strlen(tmp) + 1;
		s = (char*) malloc(len);
		memcpy(s, tmp, strlen(tmp));
		end = len -1;
	} else if (n >= 2){
		const char* s1 = va_arg(ap,char*);
		const char* s2 = va_arg(ap,char*);
		len = strlen(s1) + strlen(s2) + 1;
		s = (char*) malloc(len);
		memcpy(s, s1, strlen(s1));
		memcpy(s+strlen(s1), s2, strlen(s2));
		end = len -1;
	}

	for (int i=3; i<=n; ++i){
		const char* input = va_arg(ap,char*);
		printf("%s\n", input);
		len = end + strlen(input) + 1;
		char* new_s = (char*) realloc(s, len);
		if (new_s != NULL){
			s = new_s;
		}
		else {
			s = NULL;
			break;
		}
		memcpy(s + end, input, strlen(input));
		end = len -1;
	}
	va_end(ap);
	s[end] = '\0';
	return s;
}
