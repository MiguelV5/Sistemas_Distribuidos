from health_checker import HealthChecker
import logging

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting Health Checker")
    health_checker = HealthChecker()
    health_checker.start()
    
if __name__ == "__main__":
    main()
    