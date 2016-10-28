// +build !windows

package node

import "fmt"

/*
#include <errno.h>
#include <unistd.h>
int getErrno(void) {
	return errno;
}
*/
import "C"

var pageSize = getSystemPageSize()

func getSystemPageSize() int64 {
	sz := int64(C.sysconf(C._SC_PAGESIZE))
	if C.getErrno() == C.EINVAL {
		panic(fmt.Errorf("sysconf"))
	}
	return sz
}
