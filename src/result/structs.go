/*Common structs used by client and server */
package result

type MachineResult struct {
	Name  string
	Files []FileResult
	Err   string
}

type FileResult struct {
	Name  string
	Lines [][]string
	Err   string
}
