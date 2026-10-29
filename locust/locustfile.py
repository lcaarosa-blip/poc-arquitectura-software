import uuid
import random
from locust import HttpUser, task, between

class InventoryStressTestUser(HttpUser):
    # Tiempo de espera aleatorio entre peticiones de cada usuario virtual (100ms a 500ms)
    wait_time = between(0.1, 0.5)

    @task(3)
    def reserve_stock(self):
        """Simula la reserva masiva de stock enviando comandos al microservicio"""
        payload = {
            "orderId": str(uuid.uuid4()),
            "productId": f"PROD-{random.randint(1, 100)}",
            "quantity": random.randint(1, 5)
        }
        headers = {'Content-Type': 'application/json'}
        
        self.client.post(
            "/api/v1/inventory/reserve",
            json=payload,
            headers=headers,
            name="POST /reserve"
        )

    @task(1)
    def health_check(self):
        """Monitorea el endpoint de salud de la aplicación"""
        self.client.get("/metrics", name="GET /metrics")