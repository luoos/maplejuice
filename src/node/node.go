package node

import (
    "MembershipList"
    "net"
    "strconv"
    "time"
    "strings"
    "encoding/json"
)

PORT = ":8080"
INTRODUCER_LIST = []string{
    "fa19-cs425-g17-01.cs.illinois.edu",
    "fa19-cs425-g17-02.cs.illinois.edu",
    "fa19-cs425-g17-03.cs.illinois.edu",
    "fa19-cs425-g17-04.cs.illinois.edu",
    "fa19-cs425-g17-05.cs.illinois.edu",
    "fa19-cs425-g17-06.cs.illinois.edu",
    "fa19-cs425-g17-07.cs.illinois.edu",
    "fa19-cs425-g17-08.cs.illinois.edu",
    "fa19-cs425-g17-09.cs.illinois.edu",
    "fa19-cs425-g17-10.cs.illinois.edu",
}

type MemberInfo struct {
    method string
    id int
    ip string
    mbList MembershipList
}
type Node struct {
    id int
    mbList MembershipList
}

func (n *Node) Introduce(clientIp) {
    clientId = n.mbList.FindLeastFreeId()
    n.mbList.InsertNode(clientId, clientIp, PORT, n.GetTime())
    n.Broadcast('add', clientId, clientIp)
    mbListBuf, err = json.Marshal(n.mbList)
    if err != nil {
        log.Fatal(err)
    }
    return mbListBuf
}

func (n *Node) Join() {
    joinRequest = &MemberInfo{
        method: "join"
    }
    request, err := json.Marshal(joinRequest)
    if err != nil {
        log.Fatal()
    }
    found := false
    conn, err := nil, nil
    for introIp := range INTRODUCER_LIST {
        conn, err = net.Dial("udp", introIp + PORT)
        if err != nil {
            continue
        } else {
            break
        }
    }
    // it will at least find it self as the introducer
    if err != nil {
        log.Fatal(err)
    }
    conn.Write(request)
    buf := make([]byte, 4096)
    n, addr, err := conn.Read(buffer) // TODO: what if content exceed 4096? we need to use while loop to continuously read it.
    var mbList MembershipList
    json.Unmarshal(buf[:n], &mbList)
    n.mbList = mbList
    go n.MonitorHeartbeat()
    go n.SendHeartbeat()
}

func (n *Node) Leave() {
    n.broadcast("remove", n.id, -1)
}

func (n *Node) UDPSend(host_ip string, msg []byte) {
    conn, err := net.Dial("udp", host_ip + PORT)
    if err != nil {
        log.Println(err)
    }
    conn.Write(msg)
    conn.Close()
}
func (n *Node) SendHeartbeat() {
    hbInfo := &MemberInfo{
        method: "heartbeat",
    }
    hb, err := json.Marshal(hbInfo)
    if err != nil {
        log.Fatal(err)
    }
    for {
        for monitorNode := range n.mbList.GetNextKNodes(n.id, 3) {
            go n.UDPSend(monitorNode.ip, hb)
        }
        time.Sleep(2000 * time.Millisecond)
    }
}

func (n *Node) Broadcast(op string, target_id int, target_ip string) {
    memberInfo := &Memberinfo{
        method: op,
        id:     target_id,
        ip:     target_ip,
        // mbList: nil
    }
    data, err := json.Marshal(memberInfo)
    if err != nil {
        log.Fatal(err)
    }
    for member := range n.mbList.getNextKNodes(n.id, n.mbList.size - 1) {
            go n.UDPSend(member.ip, data)
        }
    }
}

func (n *Node) Recv() {
    conn, err := net.ListenPacket("udp", PORT)
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()
    for {
        buf := make([]byte, 4096)
        n, addr, err := conn.ReadFrom(buf)
        if err != nil {
            Log.Println(err)
        }
        var memberInfo MemberInfo
        json.Unmarshal(buf[:n], &memberInfo)
        if memberInfo.method == 'join' {
            mbListBuf := n.Introduce(memberInfo.ip)
            conn.WriteTo(mbListBuf, addr)
        } else if memberInfo.method == 'add' {
            go n.mbList.InsertNode(memberInfo.id, memberInfo.ip, "8080", n.GetTime())
        } else if memberInfo.method == 'remove' {
            go n.mbList.DeleteNode(memberInfo.id)
        } else if memberInfo.method == 'heartbeat' {
            go n.mbList.UpdateNodeHeartbeat(id, n.GetTime())
        }
    }
}

func (n *Node) MonitorHeartbeat() {
    for {
        lostIds = n.mbList.GetTimeOutNodes(n.GetTime() - 2500, n.id, 3)
        for lostId := range lostIds {
            go n.Broadcast("remove", lostId, -1)
        }
        time.Sleep(2500 * time.Millisecond) // TODO: any other way to check it every 2.5 second?
    }
}

func (n * Node) GetTime() int {
    return int(time.Now().UnixNano() / 1000000)
}
