# itda 사용 가이드 (프레젠테이션)

MCP로 itda에 배포하는 방법을 설명하는 슬라이드입니다. `index.html` 한 파일로 되어 있고,
`←/→`·스페이스·스와이프로 넘기며 `#번호`로 특정 슬라이드에 바로 갈 수 있습니다.

서비스: https://itda.rgrg.im/itda-guide/

```sh
docker build --platform linux/amd64 -t 20.20.6.248:5000/itda-guide:v2 docs/itda-guide
docker push 20.20.6.248:5000/itda-guide:v2
```
