package main

import (
	"fmt"
	"os"

	"secureledger/gateway/internal/ipc"
)

func main() {
	rw, err := ipc.NewRingWriter(os.Args[1], 1<<20)
	if err != nil { fmt.Println("open err:", err); return }
	defer rw.Close()
	raw, _ := rw.ReadRaw(0)
	fmt.Printf("go2 slot0type=%d payload_len=%d\n", raw[0], int(raw[28])|int(raw[29])<<8)
}
