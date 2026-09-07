import http from 'k6/http';
import { check, sleep } from 'k6';
export const options = { scenarios: { steady: { executor: 'constant-vus', vus: 10, duration: '30s' } }, thresholds: { http_req_failed: ['rate<0.01'], http_req_duration: ['p(95)<500'] } };
export default function () {
  const body = JSON.stringify({incident_type:'medical',severity:3,latitude:47.6062,longitude:-122.3321,description:'Synthetic k6 incident'});
  const result = http.post(`${__ENV.BASE_URL || 'http://localhost:8080'}/api/v1/incidents`, body, {headers:{'Content-Type':'application/json','Idempotency-Key':`${__VU}-${__ITER}`}});
  check(result, {'created': r => r.status === 201});
  sleep(0.2);
}

