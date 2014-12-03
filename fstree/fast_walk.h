#ifndef FAST_FIND_H
#define FAST_FIND_H

#include <dirent.h>

/*
* Prototypes
*/

typedef void (*CallBack)(const char *path, struct dirent *node);

void WalkTree(const char* path, DIR *dir, CallBack cb);

void WalkNode(const char *path, struct dirent *node, CallBack cb);


#endif /* FAST_FIND_H */
