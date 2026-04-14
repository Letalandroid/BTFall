// Fall Detection 
// August 2022 Buenos Aires, Argentina
// Attribution License
// Roni Bandini @ronibandini

/* Includes ---------------------------------------------------------------- */
#include <fallBT_inferencing.h>
#include <Arduino_BMI270_BMM150.h>
#include <ArduinoBLE.h>

BLEService myService("fff0");
BLEIntCharacteristic myCharacteristic("fff1", BLERead | BLEBroadcast);
const uint8_t completeRawAdvertisingData[] = {0x02,0x01,0x06,0x09,0xff,0x01,0x01,0x00,0x01,0x02,0x03,0x04,0x05};   
BLEAdvertisingData scanData;

#define RED 22     
#define BLUE 24     
#define GREEN 23
#define LED_PWR 25

int myCounter=0;
String worker="Smith";
int delaySeconds=5;
int mySeconds=0;

/* Constant defines -------------------------------------------------------- */
#define CONVERT_G_TO_MS2    9.80665f
#define MAX_ACCEPTED_RANGE  2.0f        // starting 03/2022, models are generated setting range to +-2, but this example use Arudino library which set range to +-4g. If you are using an older model, ignore this value and use 4.0f instead



/* Private variables ------------------------------------------------------- */
static bool debug_nn = true;
static uint32_t run_inference_every_ms = 2000;
static rtos::Thread inference_thread(osPriorityLow);
static float buffer[EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE] = { 0 };
static float inference_buffer[EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE];
static volatile uint32_t last_sample_ms = 0;
static volatile uint32_t missed_samples = 0;

/* Forward declaration */
void run_inference_background();

/**
* @brief      Arduino setup function
*/
void setup()
{

    pinMode(RED, OUTPUT);
    pinMode(BLUE, OUTPUT);
    pinMode(GREEN, OUTPUT);
    pinMode(LED_PWR, OUTPUT);
    
    Serial.begin(115200);
    lightsShow();
    
    delay(5000);
    
    Serial.println("Demo de inferencia de Edge Impulse");

    if (!IMU.begin()) {
        ei_printf("No se pudo inicializar la IMU.\r\n");
    }
    else {
        ei_printf("IMU inicializada\r\n");
    }

    if (EI_CLASSIFIER_RAW_SAMPLES_PER_FRAME != 3) {
        ei_printf("ERR: EI_CLASSIFIER_RAW_SAMPLES_PER_FRAME debe ser 3 (los 3 ejes del sensor)\n");
        return;
    }

     if (!BLE.begin()) {
    Serial.println("No se pudo inicializar BLE.");
    while (1);
  }

  myService.addCharacteristic(myCharacteristic);
  BLE.addService(myService);  

  // Build advertising data packet
  BLEAdvertisingData advData;
  // If a packet has a raw data parameter, then all the other parameters of the packet will be ignored
  advData.setRawData(completeRawAdvertisingData, sizeof(completeRawAdvertisingData));  
  // Copy set parameters in the actual advertising packet
  BLE.setAdvertisingData(advData);

  BLE.advertise();
    Serial.println("Publicidad BLE iniciada");

    advertiseNeutral(String("OK-") + worker);

    inference_thread.start(mbed::callback(&run_inference_background));
}

void lightsShow(){ 
  
  digitalWrite(RED, HIGH);  
  delay(500);                         
  digitalWrite(RED, LOW);               

  digitalWrite(BLUE, HIGH);
  delay(500);  
  digitalWrite(BLUE, LOW);

  digitalWrite(GREEN, HIGH);
  delay(500);              

}

void lightsRedOn(){ 
  digitalWrite(GREEN, LOW); 
  digitalWrite(RED, HIGH);    
}

void lightsRedOff(){ 
  digitalWrite(RED, LOW);  
  digitalWrite(GREEN, HIGH); 
}

void advertiseFall(String fallCode, int fallPct, int standPct){
  
  Serial.println("Publicando ...");

  char charBuf[50];
  String payload = fallCode + "-F" + String(fallPct) + "-S" + String(standPct);
  payload.toCharArray(charBuf, 50);
  
  scanData.setLocalName(charBuf);  
  BLE.setScanResponseData(scanData);  
  // Nombre también en GAP (no solo scan response) para que los centrales vean Fall-* al instante.
  BLE.setLocalName(charBuf);
  myCharacteristic.writeValue(fallPct);
  BLE.advertise();

}

void advertiseNeutral(const String &label){

  Serial.println("Publicando estado neutral (sin caida)...");

  char charBuf[50];
  label.toCharArray(charBuf, 50);

  scanData.setLocalName(charBuf);
  BLE.setScanResponseData(scanData);
  BLE.setLocalName(charBuf);
  BLE.advertise();

}

void killAdvertising(){
    Serial.println("Detener publicidad ...");
    BLE.stopAdvertise();  
}

float ei_get_sign(float number) {
    return (number >= 0.0) ? 1.0 : -1.0;
}


void run_inference_background()
{
    // wait until we have a full buffer
    delay((EI_CLASSIFIER_INTERVAL_MS * EI_CLASSIFIER_RAW_SAMPLE_COUNT) + 100);

    // This is a structure that smoothens the output result
    // With the default settings 70% of readings should be the same before classifying.
    ei_classifier_smooth_t smooth;
    ei_classifier_smooth_init(&smooth, 10 /* no. of readings */, 7 /* min. readings the same */, 0.8 /* min. confidence */, 0.3 /* max anomaly */);

    while (1) {
        uint32_t age_ms = millis() - last_sample_ms;
        if (last_sample_ms == 0 || age_ms > 1500) {
            // Si no hay datos frescos, evitamos inferir sobre buffer envejecido.
            delay(100);
            continue;
        }

        // copy the buffer
        memcpy(inference_buffer, buffer, EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE * sizeof(float));

        // Turn the raw buffer in a signal which we can the classify
        signal_t signal;
        int err = numpy::signal_from_buffer(inference_buffer, EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE, &signal);
        if (err != 0) {
            ei_printf("No se pudo crear la senal desde el buffer (%d)\n", err);
            return;
        }

        // Run the classifier
        ei_impulse_result_t result = { 0 };

        err = run_classifier(&signal, &result, debug_nn);
        if (err != EI_IMPULSE_OK) {
            ei_printf("ERR: No se pudo ejecutar el clasificador (%d)\n", err);
            return;
        }

        // print the predictions
        ei_printf("Predicciones ");
        ei_printf("(DSP: %d ms., Classification: %d ms., Anomaly: %d ms.)",
            result.timing.dsp, result.timing.classification, result.timing.anomaly);
        ei_printf(": ");

        // ei_classifier_smooth_update yields the predicted label
        const char *prediction = ei_classifier_smooth_update(&smooth, &result);
        ei_printf("%s ", prediction);        
        

          
        // print the cumulative results
        ei_printf(" [ ");
        for (size_t ix = 0; ix < smooth.count_size; ix++) {
            ei_printf("%u", smooth.count[ix]);
            if (ix != smooth.count_size + 1) {
                ei_printf(", ");
            }
            else {
              ei_printf(" ");
            }
        }
        ei_printf("]\n");

      for (size_t ix = 0; ix < EI_CLASSIFIER_LABEL_COUNT; ix++) {
        ei_printf("    %s: %.5f\n", result.classification[ix].label, result.classification[ix].value);
      }

        static bool bleShowsFall = false;
        int fallPct = 0;
        int standPct = 0;
        for (size_t ix = 0; ix < EI_CLASSIFIER_LABEL_COUNT; ix++) {
            if (strcmp(result.classification[ix].label, "Fall") == 0) {
                fallPct = (int)roundf(result.classification[ix].value * 100.0f);
            } else if (strcmp(result.classification[ix].label, "Stand") == 0) {
                standPct = (int)roundf(result.classification[ix].value * 100.0f);
            }
        }

        if (strcmp(prediction, "Fall") == 0) {
            if (!bleShowsFall) {
                myCounter++;
                advertiseFall(String("Fall-") + worker + "-" + String(myCounter), fallPct, standPct);
                lightsRedOn();
                bleShowsFall = true;
            }
        } else {
            if (bleShowsFall) {
                advertiseNeutral(String("OK-") + worker);
                lightsRedOff();
                bleShowsFall = false;
            }
        }

        delay(run_inference_every_ms);
    }

    ei_classifier_smooth_free(&smooth);
    
}

/**
* @brief      Get data and run inferencing
*
* @param[in]  debug  Get debug info if true
*/
void loop()
{
    while (1) {

      BLE.poll();      
  
        // Determine the next tick (and then sleep later)
        uint64_t next_tick = micros() + (EI_CLASSIFIER_INTERVAL_MS * 1000);

        float ax = 0.0f;
        float ay = 0.0f;
        float az = 0.0f;
        bool has_new_sample = false;

        if (IMU.accelerationAvailable()) {
            has_new_sample = IMU.readAcceleration(ax, ay, az);
        }

        if (!has_new_sample) {
            missed_samples++;
            if ((missed_samples % 50) == 0) {
                ei_printf("WARN: IMU sin muestra nueva (x%lu)\n", (unsigned long)missed_samples);
            }
            uint64_t now_us = micros();
            if (next_tick > now_us) {
                uint64_t time_to_wait = next_tick - now_us;
                delay((int)floor((float)time_to_wait / 1000.0f));
                delayMicroseconds(time_to_wait % 1000);
            }
            continue;
        }
        missed_samples = 0;
        last_sample_ms = millis();

        // roll the buffer -3 points so we can overwrite the last one
        numpy::roll(buffer, EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE, -3);

        // read to the end of the buffer
        buffer[EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE - 3] = ax;
        buffer[EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE - 2] = ay;
        buffer[EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE - 1] = az;

        for (int i = 0; i < 3; i++) {
            if (fabs(buffer[EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE - 3 + i]) > MAX_ACCEPTED_RANGE) {
                buffer[EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE - 3 + i] = ei_get_sign(buffer[EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE - 3 + i]) * MAX_ACCEPTED_RANGE;
            }
        }

        buffer[EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE - 3] *= CONVERT_G_TO_MS2;
        buffer[EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE - 2] *= CONVERT_G_TO_MS2;
        buffer[EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE - 1] *= CONVERT_G_TO_MS2;

        // and wait for next tick
        uint64_t now_us = micros();
        if (next_tick > now_us) {
            uint64_t time_to_wait = next_tick - now_us;
            delay((int)floor((float)time_to_wait / 1000.0f));
            delayMicroseconds(time_to_wait % 1000);
        }
    }
}