/*
 * common_utils.cpp
 *
 *  Created on: Feb 20, 2016
 *      Author: hadoop
 */

#include <common_utils.hpp>

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


