# 백 그라운드 도커 컴포즈 실행

```
docker-compse up -d
```

# 도커 컨테이너 내 카프카 접속

```
docker exec -it kafka /bin/bash
```

# 로컬 -> 도커 컨테이너 접속 시
localhost:포트 번호로 접속

# 도커 컨테이너 내부에서 사용 시
localhost:포트 번호

# 도커 컨테이너 A -> 도커 컨테이너 B로 접속 시
docker-compose.yml에 적힌 컨테이너 이름 또는 별칭으로 접속해야 함
ex)kafka:9192