// Fall Detection 
// August 2022 Buenos Aires, Argentina
// Attribution License
// Roni Bandini @ronibandini

/* Includes ---------------------------------------------------------------- */
#include <fallBT_inferencing.h>
#include <Arduino_BMI270_BMM150.h>
#include <ArduinoBLE.h>
#include <math.h>

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

/* Heurística sobre el vector aceleración (m/s²) del último frame clasificado:
 * complementa al modelo: pico/leve ingravidez o cambio de orientación vs. vertical. */
#ifndef AXES_FALL_MAG_RATIO_HIGH
#define AXES_FALL_MAG_RATIO_HIGH  1.62f  // |a| claramente > 1g (impacto / sacudida)
#endif
#ifndef AXES_FALL_MAG_RATIO_LOW
#define AXES_FALL_MAG_RATIO_LOW   0.70f  // por debajo de 1g (posible caída / transición)
#endif
#ifndef AXES_FALL_TILT_DEG_MIN
#define AXES_FALL_TILT_DEG_MIN    36.0f  // eje Z ya no domina = cuerpo / brazo muy inclinado
#endif
#ifndef FALL_PCT_BYPASS_AXES
#define FALL_PCT_BYPASS_AXES      90     // si el modelo está muy seguro, no exigir ejes
#endif
#ifndef FALL_PCT_TRIGGER
#define FALL_PCT_TRIGGER          60     // entrar en alarma Fall si Fall% >= esto (raw)
#endif
#ifndef FALL_PCT_RELEASE
#define FALL_PCT_RELEASE          42     // salir a OK solo si Fall% cae por debajo (histéresis; evita parpadeo)
#endif

/* Giroscopio (readGyroscope: rad/s en Nano 33 BLE Rev2): rotación brusca suele acompañar caída/torsión. */
#ifndef GYRO_MAG_RAD_S_MIN
#define GYRO_MAG_RAD_S_MIN        2.2f   // |ω| total por encima de esto cuenta como evento (≈126°/s)
#endif
#ifndef PRINT_IMU_DEBUG_LINE
#define PRINT_IMU_DEBUG_LINE      0      // 1 = una línea Serial con acc+gyro tras cada inferencia (mucha carga UART)
#endif

/* Private variables ------------------------------------------------------- */
/* debug_nn=true imprime el vector de features completo dentro de run_classifier
 * (carga muy alta en UART; puede disparar tiempos DSP aparentes de segundos). */
static bool debug_nn = false;
static uint32_t run_inference_every_ms = 2000;
static rtos::Thread inference_thread(osPriorityNormal);
static float buffer[EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE] = { 0 };
static float inference_buffer[EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE];
static volatile uint32_t missed_samples = 0;
/* Última muestra de giro (rad/s), alineada con el último triplete acc del buffer EI. */
static volatile float last_gx = 0.0f;
static volatile float last_gy = 0.0f;
static volatile float last_gz = 0.0f;
static volatile bool last_gyro_valid = false;

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
    
    Serial.println("Edge Impulse Inferencing Demo");

    if (!IMU.begin()) {
        ei_printf("Failed to initialize IMU!\r\n");
    }
    else {
        ei_printf("IMU initialized\r\n");
    }

    if (EI_CLASSIFIER_RAW_SAMPLES_PER_FRAME != 3) {
        ei_printf("ERR: EI_CLASSIFIER_RAW_SAMPLES_PER_FRAME should be equal to 3 (the 3 sensor axes)\n");
        return;
    }

     if (!BLE.begin()) {
    Serial.println("failed to initialize BLE!");
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
    Serial.println("BLE advertising started");

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
  
  Serial.println("Advertising ...");

  char charBuf[50];
  String payload = fallCode + "-F" + String(fallPct) + "-S" + String(standPct);
  payload.toCharArray(charBuf, 50);
  
  scanData.setLocalName(charBuf);  
  BLE.setScanResponseData(scanData);  
  // Nombre también en GAP (no solo scan response) para que los centrales vean Fall-* al instante.
  BLE.setLocalName(charBuf);
  BLE.advertise();

}

void advertiseNeutral(const String &label){

  Serial.println("Advertising neutral (no fall)...");

  char charBuf[50];
  label.toCharArray(charBuf, 50);

  scanData.setLocalName(charBuf);
  BLE.setScanResponseData(scanData);
  BLE.setLocalName(charBuf);
  BLE.advertise();

}

void killAdvertising(){
    Serial.println("Stop advertising ...");
    BLE.stopAdvertise();  
}

float ei_get_sign(float number) {
    return (number >= 0.0) ? 1.0 : -1.0;
}

/**
 * Heurística física además del modelo EI: aceleración (m/s²) + opcionalmente giro (rad/s).
 * Devuelve true si hay señal de impacto, inclinación fuerte o rotación significativa.
 */
bool axes_support_fall_decision(float ax, float ay, float az, float gx, float gy, float gz, bool gyro_valid)
{
    const float g = CONVERT_G_TO_MS2;
    float mag = sqrtf(ax * ax + ay * ay + az * az);
    if (mag < 0.35f * g) {
        return false;
    }

    float ratio = mag / g;
    bool magnitude_event = (ratio >= AXES_FALL_MAG_RATIO_HIGH) || (ratio <= AXES_FALL_MAG_RATIO_LOW);

    float zu = fabsf(az) / mag;
    if (zu > 1.0f) {
        zu = 1.0f;
    }
    float tilt_deg = acosf(zu) * (180.0f / 3.14159265f);
    bool orientation_event = (tilt_deg >= AXES_FALL_TILT_DEG_MIN);

    bool gyro_event = false;
    if (gyro_valid) {
        float gyro_mag = sqrtf(gx * gx + gy * gy + gz * gz);
        gyro_event = (gyro_mag >= GYRO_MAG_RAD_S_MIN);
    }

    return magnitude_event || orientation_event || gyro_event;
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
        // copy the buffer
        memcpy(inference_buffer, buffer, EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE * sizeof(float));

        // Turn the raw buffer in a signal which we can the classify
        signal_t signal;
        int err = numpy::signal_from_buffer(inference_buffer, EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE, &signal);
        if (err != 0) {
            ei_printf("Failed to create signal from buffer (%d)\n", err);
            return;
        }

        // Run the classifier
        ei_impulse_result_t result = { 0 };

        err = run_classifier(&signal, &result, debug_nn);
        if (err != EI_IMPULSE_OK) {
            ei_printf("ERR: Failed to run classifier (%d)\n", err);
            return;
        }

        // print the predictions
        ei_printf("Predictions ");
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

      int fallPct = 0;
      int standPct = 0;
      for (size_t ix = 0; ix < EI_CLASSIFIER_LABEL_COUNT; ix++) {
        if (strcmp(result.classification[ix].label, "Fall") == 0) {
          fallPct = (int)roundf(result.classification[ix].value * 100.0f);
        } else if (strcmp(result.classification[ix].label, "Stand") == 0) {
          standPct = (int)roundf(result.classification[ix].value * 100.0f);
        }
      }

        /* Último triplete = última muestra IMU usada en este frame (m/s²). */
        float last_ax = inference_buffer[EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE - 3];
        float last_ay = inference_buffer[EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE - 2];
        float last_az = inference_buffer[EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE - 1];
        float gx_snap = last_gx;
        float gy_snap = last_gy;
        float gz_snap = last_gz;
        bool gyro_snap = last_gyro_valid;
        bool axes_ok = axes_support_fall_decision(
            last_ax, last_ay, last_az, gx_snap, gy_snap, gz_snap, gyro_snap);
        /* Con umbral raw >= TRIGGER no exigir IMU (si no, un frame >60% y el siguiente <60% volvía a OK). */
        bool bypass_axes = (fallPct >= FALL_PCT_BYPASS_AXES) || (fallPct >= FALL_PCT_TRIGGER);

#if PRINT_IMU_DEBUG_LINE
        ei_printf(
            "IMU acc(m/s²) ax=%.2f ay=%.2f az=%.2f | gyro(rad/s) gx=%.3f gy=%.3f gz=%.3f valid=%d\n",
            (double)last_ax, (double)last_ay, (double)last_az,
            (double)gx_snap, (double)gy_snap, (double)gz_snap, gyro_snap ? 1 : 0);
#endif

        static bool bleShowsFall = false;
        bool enter_fall = (strcmp(prediction, "Fall") == 0) || (fallPct >= FALL_PCT_TRIGGER);
        /* Mientras la alarma está activa, no volver a OK por un solo frame con Fall% <60% (histéresis). */
        bool leave_fall = (fallPct < FALL_PCT_RELEASE);

        if (!bleShowsFall) {
            if (enter_fall && (axes_ok || bypass_axes)) {
                myCounter++;
                advertiseFall(String("Fall-") + worker + "-" + String(myCounter), fallPct, standPct);
                lightsRedOn();
                bleShowsFall = true;
            } else if (debug_nn && enter_fall && !axes_ok && !bypass_axes) {
                ei_printf(
                    "Fall modelo pero sensores no confirman acc=(%.2f,%.2f,%.2f) m/s² gyro=(%.3f,%.3f,%.3f) rad/s valid=%d\n",
                    (double)last_ax, (double)last_ay, (double)last_az,
                    (double)gx_snap, (double)gy_snap, (double)gz_snap, gyro_snap ? 1 : 0);
            }
        } else if (leave_fall) {
            advertiseNeutral(String("OK-") + worker);
            lightsRedOff();
            bleShowsFall = false;
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
                ei_printf("WARN: IMU without fresh sample (x%lu)\n", (unsigned long)missed_samples);
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

        float gx = 0.0f;
        float gy = 0.0f;
        float gz = 0.0f;
        bool gyro_ok = false;
        if (IMU.gyroscopeAvailable()) {
            gyro_ok = IMU.readGyroscope(gx, gy, gz);
        }

        // roll the buffer -3 points so we can overwrite the last one
        numpy::roll(buffer, EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE, -3);

        // write fresh sample to the end of the buffer
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

        /* Misma marca temporal que el último triplete (para heurística giro + aceleración). */
        if (gyro_ok) {
            last_gx = gx;
            last_gy = gy;
            last_gz = gz;
            last_gyro_valid = true;
        } else {
            last_gyro_valid = false;
        }

        // and wait for next tick
        uint64_t now_us = micros();
        if (next_tick > now_us) {
            uint64_t time_to_wait = next_tick - now_us;
            delay((int)floor((float)time_to_wait / 1000.0f));
            delayMicroseconds(time_to_wait % 1000);
        }
    }
}