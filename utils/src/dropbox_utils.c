#include <dropbox_utils.h>
#include <time.h>
#include <inttypes.h>
#include <common_utils.h>
#include <log.h>

int parse_mon(char* mon_str){
	int mon = 0;
	if (strncmp(mon_str, "Jan", 3) == 0){
		mon = 0;
	} else if (strncmp(mon_str, "Feb", 3) == 0){
		mon = 1;
	} else if (strncmp(mon_str, "Mar", 3) == 0){
		mon = 2;
	} else if (strncmp(mon_str, "Apr", 3) == 0){
		mon = 3;
	} else if (strncmp(mon_str, "May", 3) == 0){
		mon = 4;
	} else if (strncmp(mon_str, "Jun", 3) == 0){
		mon = 5;
	} else if (strncmp(mon_str, "Jul", 3) == 0){
		mon = 6;
	} else if (strncmp(mon_str, "Aug", 3) == 0){
		mon = 7;
	} else if (strncmp(mon_str, "Sep", 3) == 0){
		mon = 8;
	} else if (strncmp(mon_str, "Oct", 3) == 0){
		mon = 9;
	} else if (strncmp(mon_str, "Nov", 3) == 0){
		mon = 10;
	} else if (strncmp(mon_str, "Dec", 3) == 0){
		mon = 11;
	}
	return mon;
}

long parse_time(char* time_string){
	struct tm t;
	time_t t_of_day;
	const char* day_str = substring(time_string, 5,7);
	const char* mon_str = substring(time_string, 8,11);
	const char* year_str = substring(time_string, 12,16);
	const char* hour_str = substring(time_string, 17,19);
	const char* min_str = substring(time_string, 20,22);
	const char* sec_str = substring(time_string, 23,25);

	char* endptr;

	uintmax_t day = strtoumax(day_str, &endptr, 10);
	int mon = parse_mon(mon_str);
	uintmax_t year = strtoumax(year_str, &endptr, 10);
	uintmax_t hour = strtoumax(hour_str, &endptr, 10);
	uintmax_t min = strtoumax(min_str, &endptr, 10);
	uintmax_t sec = strtoumax(sec_str, &endptr, 10);

	t.tm_year = year - 1900;
	t.tm_mon = mon;           // Month, 0 - jan
	t.tm_mday = day;
	t.tm_hour = (hour -1 + 20) % 24 + 1;
	log_msg("hour: %ld", hour);
	t.tm_min = min;
	t.tm_sec = sec;
	t.tm_isdst = 0;        // Is DST on? 1 = yes, 0 = no, -1 = unknown
	t_of_day = mktime(&t);

	return (long)t_of_day;
}


