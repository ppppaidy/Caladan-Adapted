/*
 * bw_config.h - Breakwater configurations
 */

#pragma once

/* Recommended parameters (in XL170 environment)
* - 1 us average service time
* #define SBW_MIN_DELAY_US		45
* #define SBW_DROP_THRESH		90
*
* - 10 us average service time
* #define SBW_MIN_DELAY_US		80
* #define SBW_DROP_THRESH		160
*
* - 100 us average service time
* #define SBW_MIN_DELAY_US		500
* #define SBW_DROP_THRESH		1000
*/

/* delay threshold to detect congestion */
#define SBW_DELAY_TARGET		80
/* delay threshold for AQM */
#define SBW_DROP_THRESH			160

/* round trip time in us */
#define SBW_RTT_US			10
#define SBW_AI				0.001
#define SBW_MD				0.02

/* the maximum supported window size */
#define SBW_MAX_WINDOW_EXP		6
#define SBW_MAX_WINDOW			64

#define CBW_MAX_CLIENT_DELAY_US		10

/* scales the targeted reduction in credits before allowing a core to park */
// moving to be initialzed by server.config instead
// #define SBW_CORE_PARK_TARGET 1.0
