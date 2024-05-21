import json
import requests
from confluent_kafka import Producer
import time

# Kafka 서버 및 토픽 설정
servers = ['kafka_node1:9092', 'kafka_node2:9092', 'kafka_node3:9092'] # Kafka 브로커:포트
topic_name = 'bike-station-info' # 사용할 Kafka 토픽 이름

# 메시지 전송 결과 처리 함수
#def delivery_report(err, msg):
 #   if err is not None:
  #      print('Message delivery failed: {}'.format(err))
   # else:
    #    print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# Kafka Producer 생성
producer = Producer({'bootstrap.servers': ','.join(servers)})

# 데이터 가져오기
def fetch_data(start_idx, end_idx):
    api_server = 'http://openapi.seoul.go.kr:8088/4c707448636a696837364669504d4e/json/bikeList/{}/{}'.format(start_idx, end_idx)
    response = requests.get(api_server)
    data = json.loads(response.content)
    return data

# 페이지별로 데이터 가져와서 Kafka에 전송
def send_data():
    start_idx = 1
    end_idx = 1000
    while start_idx <= 2000:
        data = fetch_data(start_idx, end_idx)
        for station in data['rentBikeStatus']['row']:
            # 필요한 데이터 추출
            rack_tot_cnt = station['rackTotCnt']
            station_name = station['stationName']
            parking_bike_tot_cnt = station['parkingBikeTotCnt']
            shared = station['shared']
            station_latitude = station['stationLatitude']
            station_longitude = station['stationLongitude']
            station_id = station['stationId']

            # 데이터를 JSON 형식으로 변환
            message = {
                'rack_tot_cnt': rack_tot_cnt,
                'station_name': station_name,
                'parking_bike_tot_cnt': parking_bike_tot_cnt,
                'shared': shared,
                'station_latitude': station_latitude,
                'station_longitude': station_longitude,
                                'station_id': station_id
            }
            json_data = json.dumps(message)

            # Kafka에 메시지 전송
            producer.poll(0) # 이벤트 처리
            producer.produce(topic_name, json_data.encode('utf-8')) # 메시지 전송
            producer.flush() # 메시지 전송 완료

            # 전송한 데이터를 출력
            print(f"Sent data to Kafka: {message}")

        start_idx += 1000
        end_idx += 1000
        time.sleep(30) # 30초마다 실행

# 메인 함수
def main():
    send_data()

if __name__ == "__main__":
    main()