/*Copyright 2014 Computer Vision Lab

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

/**
 * A Pig script that defines the data mining package UDFs.
 * @author Patrick Happ
 */

DEFINE II_Mean br.puc_rio.ele.lvc.interimage.data.MeanValue;
DEFINE II_Brightness br.puc_rio.ele.lvc.interimage.data.BrightnessValue;
DEFINE II_MaxPixVal br.puc_rio.ele.lvc.interimage.data.MaxPixelValue;
DEFINE II_MinPixVal br.puc_rio.ele.lvc.interimage.data.MinPixelValue;
DEFINE II_Variance br.puc_rio.ele.lvc.interimage.data.VarianceValue;
DEFINE II_StdDev br.puc_rio.ele.lvc.interimage.data.StdDevValue;
DEFINE II_Median br.puc_rio.ele.lvc.interimage.data.MedianValue;
DEFINE II_Amplitude br.puc_rio.ele.lvc.interimage.data.AmplitudeValue;
DEFINE II_Sum br.puc_rio.ele.lvc.interimage.data.SumPixelValue;
DEFINE II_Ratio br.puc_rio.ele.lvc.interimage.data.RatioValue;
DEFINE II_Covariance br.puc_rio.ele.lvc.interimage.data.CovarValue;
DEFINE II_Correlation br.puc_rio.ele.lvc.interimage.data.CorrelValue;
DEFINE II_Mode br.puc_rio.ele.lvc.interimage.data.ModeValue;
DEFINE II_Entropy br.puc_rio.ele.lvc.interimage.data.EntropyValue;
DEFINE II_GLCMMean br.puc_rio.ele.lvc.interimage.data.MeanGLCM;
DEFINE II_GLCMContrast br.puc_rio.ele.lvc.interimage.data.ContrastGLCM;
DEFINE II_GLCMASM br.puc_rio.ele.lvc.interimage.data.ASMGLCM;
DEFINE II_GLCMIDM br.puc_rio.ele.lvc.interimage.data.IDMGLCM;
DEFINE II_GLCMEntropy br.puc_rio.ele.lvc.interimage.data.EntropyGLCM;
DEFINE II_GLCMHomogeinity br.puc_rio.ele.lvc.interimage.data.HomogeinityGLCM;
DEFINE II_GLCMVariance br.puc_rio.ele.lvc.interimage.data.VarianceGLCM;
DEFINE II_GLCMStdDev br.puc_rio.ele.lvc.interimage.data.StdDevGLCM;
DEFINE II_GLCMSDissimilarity br.puc_rio.ele.lvc.interimage.data.DissimilaritytGLCM;
DEFINE GLCMQuiSquare br.puc_rio.ele.lvc.interimage.data.QuiSquaretGLCM;
