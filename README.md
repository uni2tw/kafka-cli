# Kafka CLI

Kafka 維運用的命令列工具，Fork 自開源專案 [olesmartyniuk/kafka-cli](https://github.com/olesmartyniuk/kafka-cli) 並持續擴充。除了查 topic、收發訊息這些基本功能，最主要是拿來對付事件驅動架構下常見的維運場景：某個 consumer group（EventHandler）漏處理一筆事件需要補、卡在一筆處理失敗的事件需要跳過、或是要追一筆訊息當初的內容跟落點。

## 安裝 / 建置

需要 .NET 9 SDK。

```bash
dotnet build
```

打包成單一執行檔（win-x64 + linux-x64，自帶 runtime）：

```bash
_build_single_win10.bat
```

win-x64 的版本會自動複製一份到 `publish\kafka-cli.exe`（會直接覆蓋舊檔）。

## 設定

第一次執行任何指令前，先設定要連線的 broker：

```
kafka-cli config
```

會問你目前設定，並詢問是否要更新 broker host 跟 timeout。設定存在使用者家目錄的 `~/.kafka/config`（ini 格式），不會寫進程式碼或 repo 裡。

## 互動式 Shell（推薦的使用方式）

```
kafka-cli shell
```

日常維運建議都從這裡進去，而不是每次都打一長串 `kafka-cli message ...`。進入 REPL 之後可以省略 `kafka-cli` 前綴直接下指令，並且有以下輔助功能：

- **指令捷徑**：`get`/`find`/`groups`/`offset` 會自動補上 `message`/`consumer` 前綴，例如直接打 `get 100` 就等於 `message get 100`。
- **預設值（context）**：`topic MyTopic` 設定這個 session 的預設 topic（同時會列出訂閱它的 consumer group，可直接選號設成預設 group）；`use group MyGroup` 設定預設 group；`use clear` 清除。設好之後 `get`/`find`/`consumer offset` 這類指令可以省略 `-t`/`-g`，不用每次重打完整的 topic/group 名稱。
- **Tab 補全**：topic、group、partition 都會即時查詢候選清單；topic 名稱用 `.` 分段比對（例如打 `Internal` 能補出 `PX.Internal.Stocks`），不需要打完整前綴；同一次 Tab 若還有多個候選，會直接把清單列出來，不用按第二次。
- **輸出重導向**：`指令 > file.txt`（覆蓋寫檔）、`指令 >> file.txt`（附加寫檔）、`指令 | tee file.txt`（畫面 + 檔案都要），語法跟一般 shell 一致。
- **指令歷史**：上下鍵翻歷史，`history` 列出完整清單。
- **其他內建指令**：`context`（看目前的預設值）、`clear`/`cls`、`help`/`?`、`exit`/`quit`。

Shell 內輸入 `help` 可以看到完整的指令速查表跟範例（包含 `get`/`find` 更進階的用法）。

範例（設定預設 topic/group 後連續操作，不用每次重打 `-t`/`-g`）：

```
kafka[uat-kafka:9092]> topic PX.Internal.Stocks
Default topic set to 'PX.Internal.Stocks'.
Handlers(groups) for 'PX.Internal.Stocks':
  1. PX.BAS.Events.StockSyncEventHandler
Select group number or name and press Enter (empty to skip): 1
Default group set to 'PX.BAS.Events.StockSyncEventHandler'.
kafka[uat-kafka:9092]> get -1
kafka[uat-kafka:9092]> find 關鍵字 -n 20 | tee result.txt
kafka[uat-kafka:9092]> offset -ofc 1
```

## 指令參考（也可以不進 Shell，單次執行）

```
kafka-cli topic                          列出所有 topic
kafka-cli topic -f MyFilter               列出名稱包含 MyFilter 的 topic

kafka-cli message get -t MyTopic 100      取得 offset 100 的訊息
kafka-cli message get -t MyTopic -o -1    取得最新一筆訊息（負數索引，見下方說明）
kafka-cli message find -t MyTopic 關鍵字   依關鍵字搜尋訊息
kafka-cli message find-path -t MyTopic -so 600 /SalesMix/SaleCode   依 JSON path 搜尋
kafka-cli message produce "訊息內容" -t MyTopic   發送一筆訊息
kafka-cli message consume -t MyTopic       持續消費並印出訊息（Ctrl+C 結束）
kafka-cli message clone -t MyTopic -o 100 -tt TargetTopic   把一筆訊息複製到另一個 topic
kafka-cli message remote-copy -t MyTopic -sh source:9092 -th target:9092 -so 100 -eo 200   跨叢集搬移一段訊息

kafka-cli consumer groups -t MyTopic       列出訂閱這個 topic 的 consumer group
kafka-cli consumer offset -t MyTopic -g MyGroup -o 54      把 group 的 committed offset 設成絕對值 54
kafka-cli consumer offset -t MyTopic -g MyGroup -ofc 1     把 offset 往前推進 1（跳過 1 筆事件不處理）
kafka-cli consumer offset -t MyTopic -g MyGroup -ofc -100  把 offset 往回倒退 100（重新消費最近 100 筆）
```

### get / clone 的負數索引

`-o|--offset` 支援負數，代表「從這個 partition 最新一筆往回數」：`-1` 是最新一筆、`-100` 是倒數第 100 筆。因為值開頭是 `-`，只能透過 `-o` 這個具名選項傳，不能直接當位置參數（`get -1` 這種寫法會被誤判成未知選項，一定要寫成 `get -o -1`）。沒指定 `-p/--partition` 時，會把每個 partition 各自的結果都列出來（各 partition 的 offset 是獨立遞增的，沒有跨 partition 的全域順序）。

### find 的關鍵字語法

```
find -t MyTopic 關鍵字        關鍵字必須出現（等同 +關鍵字）
find -t MyTopic *             不篩選，全部列出
find -t MyTopic A-B           A 必須出現，B 必須不出現
find -t MyTopic A+B           A、B 都必須出現
```

搭配 `-n/--ntop` 限制筆數、`-s/--start`、`-e/--end` 限制時間區間、`-so` 額外印出 `/*p{partition}:{offset}*/`、`-p/--path` 抽取指定 JSON 欄位而非印整包訊息。

### consumer offset 的兩種寫法

- `-o|--offset`：絕對值，直接把 committed offset 設成這個數字。
- `-ofc|--offsetFromCurrent`：相對值，以目前 committed offset 為基準做加減，可以是負數（往回倒退）。每個 partition 各自計算，不是對整個 topic 只移動這個數字。

**注意**：這兩個都是直接改 Kafka 上 `__consumer_offsets` 的紀錄。如果對應的 consumer（EventHandler）還在跑，改了不保證有效——它可能會被自己下一次的 auto-commit 蓋掉，甚至因為 generation 不合法被 broker 拒絕。建議先確認該 consumer 沒有 active member 在跑，再進行調整。

## 專案結構

```
Config/     設定檔讀寫（~/.kafka/config）
Consumer/   consumer group 查詢、offset 調整
Kafka/      Kafka client 封裝（KafkaClient）、共用工具
Message/    訊息的 get/find/produce/consume/clone/remote-copy
Shell/      互動式 REPL（補全、行編輯、輸出重導向、context）
Topic/      topic 列表
```
