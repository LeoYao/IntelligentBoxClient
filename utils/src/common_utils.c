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

int compare_string(const char* s1, const char* s2){

	if (s1 == NULL || s2 == NULL) {
		if (s1 == NULL && s2 == NULL){
			return 0;
		} else {
			return -1;
		}
	}

	if (strlen(s1)) == strlen(s2) && strncmp(s1, s2, strnlen(s1)){
		return 0;
	} else {
		return -1;
	}
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


int expand_mem(void** ptr, int size){
	if (ptr == NULL || size == 0){
		return -1;
	}
	void* result = realloc(*ptr, size * 2);
	if (result != NULL){
		*ptr = result;
		return 0;
	}
	return -1;
}

char* copy_text(const char* tmp){
	char* result = NULL;
	if (tmp != NULL){
		int len = strlen(tmp);
		result = (char*)malloc((len + 1)*sizeof(char));
		strncpy(result, tmp, len);
		result[len] = '\0';
	} else {
		result = (char*)malloc(sizeof(char));
		result[0] = '\0';
	}

	return result;
}

char* substring(const char* str, int start, int end){
	char* result = NULL;
	if (str != NULL && start < end){
		int len = end - start;
		result = (char*)malloc((len + 1)*sizeof(char));
		strncpy(result, str+start, len);
		result[len] = '\0';
	} else {
		result = (char*)malloc(sizeof(char));
		result[0] = '\0';
	}

	return result;
}

char* get_parent_path(const char* path){
	char* parent_path = NULL;

	int len = strlen(path);
	for (int i = len - 1; i >= 0; i--){
		if (path[i] == '/'){
			parent_path = substring(path, 0, i);
			break;
		}
	}

	if (parent_path == NULL){
		parent_path = (char*)malloc(sizeof(char));
		parent_path[0] = '\0';
	}

	return parent_path;
}

char* get_file_name(const char* path){
	char* file_name = NULL;

	int len = strlen(path);
	for (int i = len - 1; i >= 0; i--){
		if (path[i] == '/'){
			file_name = substring(path, i+1, len);
			break;
		}
	}

	if (file_name == NULL){
		file_name = (char*)malloc(sizeof(char));
		file_name[0] = '\0';
	}

	return file_name;
}

long get_current_epoch_time(){
	time_t rawtime;
	time(&rawtime);
	return (long)rawtime;
}
