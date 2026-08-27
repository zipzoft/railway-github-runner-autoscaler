## หลักฐาน

```
go test ./... -race -count=1     # 106 tests, ผ่านติดกัน 5 รอบ + shuffle 3 รอบ
go vet ./... && gofmt -l .       # สะอาด
docker build .                   # ผ่าน (ดูด้านล่าง)
```

RED เก็บก่อนเสมอ — เขียน `reconcile` เป็น stub เปล่าก่อน มี 6 เทสต์แดง แล้วค่อยเติมของจริง

**mutation 34 ตัว แต่ละตัวฆ่าเทสต์ที่ระบุชื่อได้จริง** เช่น prune-on-404, prune-a-running-job,
CAS-on-timestamp-not-identity, adopt-without-proving-the-repo-readable, a-404-marks-a-repo-readable,
no-wall-clock-budget, apply's-boot-floor-clamp-removed, repo-pattern-allows-traversal

มีหลายเทสต์ที่ตอนแรก **ผ่านด้วยเหตุผลผิด** แล้วหนูเขียนใหม่จนกว่า mutation ที่มันอ้างจะฆ่ามันได้จริง — ตัวหนึ่ง
(`TestReconcile_CannotShrinkTheFleetBelowTheBootEraFloor`) **แดงไม่ได้เลย** เพราะวน loop บน list ที่ว่างหลัง tick
ตอนนี้มี positive control ที่ข้าม horizon แล้วบังคับให้ต้องหด — เพื่อแยก "floor กันไว้" ออกจาก "ไม่มีอะไรเกิดขึ้น"

## ⭐ เจอตอนจะ deploy: Dockerfile พัง

`Dockerfile` ไล่ copy ไฟล์ทีละชื่อ (`COPY main.go .` / `COPY server.go .`) — `github.go` เลยไม่เคยถูก copy
`docker build` **พังทันที** (`undefined: jobEntry`) และ **CI จับไม่ได้** เพราะ job build image ติด
`if: github.event_name != 'pull_request'` → PR ไม่เคย build image เลย

เทสต์เขียว CI เขียว แต่ artifact deploy ไม่ได้ เจอเพราะหนูรัน `docker build` **ก่อน** deploy ไม่ใช่หลัง
แก้เป็น `COPY *.go ./` แล้วให้ build image ตอน PR ด้วย (build ไม่ push) จะได้เป็น check ไม่ใช่ deploy ที่ล้ม

smoke test บน image จริง: `/health` 200, ไม่มี token → log บอก `reconcile DISABLED` ตามที่อ้าง,
ใส่ token ผิด → `[WARN] GITHUB_TOKEN did not authenticate (github /rate_limit returned 401)` ตอน boot ทันที

## gate ที่ผ่าน

plan review + diff review บน fresh-context subagent, และ scrutinize pass แยกอีกตัว — ทั้งสาม
รอบเจอของจริงทุกรอบ รวมถึงสองข้อที่ใหญ่กว่าตัว feature เอง (adoption gate, เทสต์ที่แดงไม่ได้) แก้ครบแล้ว
