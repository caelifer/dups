#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <err.h>

#include "fast_walk.h"

#ifndef NAME_MAX
#define NAME_MAX 256
#endif

static struct dirent *createNode(const char *path);
static int dots(const char *name);

/*
* Implementation
*/

// WalkTree
void WalkTree(const char *path, DIR *dir, CallBack cb) {
	struct dirent node, *result;

	// Read all entries one by one
	for (result = &node; readdir_r(dir, &node, &result) == 0 && result != NULL;) {
		// Skip . && ..
		if (dots(node.d_name)) {
			continue;
		}

		// Construct path
		int sz = strlen(path) + strlen(node.d_name) + 2; // +2 for last '0' and middle '/'

		char *newPath = malloc(sizeof(char) * sz);
		if (newPath == NULL) {
			perror("memory [WT]");
			return;
		}

		// Finally build new path
		sprintf(newPath, "%s/%s", path, node.d_name);

		// Walk each node
		WalkNode(newPath, &node, cb);

		// Make sure to free allocated newPath
		free(newPath);
	}

	// Process errors
	if (result == NULL) {
		// EOF - done
		return;
	} else {
		perror(path);
	}
}

// WalkNode
void WalkNode(const char *path, struct dirent *node, CallBack cb) {
	int needFree = 0;

	// If node is NULL, populate node
	if (node == NULL) {
		if ((node = createNode(path)) == NULL) {
			perror("node");
			return;
		}
		needFree = 1;
	}

	// At this point our data structures are fully populated
	
	// First run call back
	cb(path, node);

	// Check if node is directory and call WalkTree on it
	if (node->d_type == DT_DIR) {

		DIR *dir = opendir(path);

		if (dir == NULL) {
			// perror("opendir");
			warn("'%s'", path);
			return;
		}

		WalkTree(path, dir, cb);

		// Always close open directory
		closedir(dir);
	}

	if (needFree) {
		free(node); // Free node if we created it
	}
}

static int dots(const char *name) {
	return strncmp(name, "..", NAME_MAX) == 0 || strncmp(name, ".", NAME_MAX) == 0;
	// return strncmp(name, "..", NAME_MAX) == 0;
}

static struct dirent *createNode(const char *path) {
		struct dirent *node;
		struct stat buf;

		// Populate node by doing lstat(2)
		node = malloc(sizeof(struct dirent));
		if (node == NULL) {
			perror("memory [WN]");
			return NULL;
		}

		// Get stats
		if (lstat(path, &buf) == -1) {
			perror("lstat");
			return NULL;
		}

		// Set node attributes
		char *p = rindex(path, '/');
		strncpy(node->d_name, (p == NULL) ? path : p+1, NAME_MAX); // empty string is a name for system root

		// Set inode
		node->d_ino = buf.st_ino;

		// Set type
		if (S_ISREG(buf.st_mode)) {
			node->d_type = DT_REG;
		} else if (S_ISDIR(buf.st_mode)) {
			node->d_type = DT_DIR;
		} else if (S_ISCHR(buf.st_mode)) {
			node->d_type = DT_CHR;
		} else if (S_ISBLK(buf.st_mode)) {
			node->d_type = DT_BLK;
		} else if (S_ISFIFO(buf.st_mode)) {
			node->d_type = DT_FIFO;
		} else if (S_ISLNK(buf.st_mode)) {
			node->d_type = DT_LNK;
		} else if (S_ISSOCK(buf.st_mode)) {
			node->d_type = DT_SOCK;
		} else {
			node->d_type = DT_UNKNOWN;
		}

		return node;
}

// Disable main compiliation
#ifdef TTT_XXX_TTT
// Our implimentation of CallBack
void printNode(const char *path, struct dirent *node) {
	char *type = node->d_type == DT_DIR ? "DIR" : "OTH";
	printf("[%s] %s\n", type, path);
}

int main(int argc, char *argv[]) {
	if (argc == 1) {
		WalkNode(".", NULL, printNode);
	} else {
		int i;
		for (i = argc - 1; i > 0; i--) {
			WalkNode(*(++argv), NULL, printNode);
		}
	}
}
#endif
