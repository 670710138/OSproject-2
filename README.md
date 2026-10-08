# Multi-threaded File Download Client/Server (Java 21)

## Compile
    javac -d classes FileServer.java FileClient.java

## Run
    # สร้างโฟลเดอร์ไฟล์ที่แชร์ + ไฟล์ทดสอบ (ทำแค่ครั้งเดียว)
    mkdir share
    powershell -c "$b=New-Object byte[] 536870912; (New-Object Random).NextBytes($b); [IO.File]::WriteAllBytes('share\test.bin',$b)"

    # Server : java FileServer <port> <dir> [io|nio] [virtual|N]
    java -cp classes FileServer 9090 share io virtual
    java -cp classes FileServer 9090 share nio virtual

    # Client
    java -cp classes FileClient localhost 9090 list
    java -cp classes FileClient localhost 9090 info test.bin

    java -cp classes FileClient localhost 9090 download test.bin io 1 3 downloads
    java -cp classes FileClient localhost 9090 download test.bin io 10 3 downloads

    java -cp classes FileClient localhost 9090 download test.bin nio 1 3 downloads
    java -cp classes FileClient localhost 9090 download test.bin nio 10 3 downloads
    #                                     <file>       <mode> <workers> <repeat> <outDir>

## Benchmark อัตโนมัติ (Linux/macOS/Git-Bash)
    ./run_benchmark.sh 512      # ได้ results.csv

## Protocol
| Client ส่ง | Server ตอบ |
|---|---|
| `LIST` | `FILE <name> <size>` ... แล้วจบด้วย `END` |
| `INFO <file>` | `SIZE <bytes>` หรือ `ERROR 404 ...` |
| `HASH <file>` | `HASH <sha256>` (ใช้ตรวจความถูกต้อง) |
| `GET <file> <offset> <length>` | `OK <length>` + payload ตามจำนวน byte หรือ `ERROR <code> <msg>` |
| `QUIT` | `BYE` |

ERROR code: 400 request ผิดรูปแบบ / 404 ไม่พบไฟล์ / 416 ช่วงข้อมูลไม่ถูกต้อง
"# OSproject---2" 
