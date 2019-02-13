package job

// Interface is a wrapper type to represent a unit of work, usually captured in a function closure.
// It is this function/closure responsibility to communicate its return value by external
// means.
type Interface func()
