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
 * A Pig script that defines the geometry package UDFs.
 * @author Rodrigo Ferreira *
 */

--Eval UDFs
DEFINE II_Area br.puc_rio.ele.lvc.interimage.geometry.udf.Area;
DEFINE II_AreaOf br.puc_rio.ele.lvc.interimage.geometry.udf.AreaOf;
DEFINE II_BorderTo br.puc_rio.ele.lvc.interimage.geometry.udf.BorderTo;
DEFINE II_Buffer br.puc_rio.ele.lvc.interimage.geometry.udf.Buffer;
DEFINE II_Centroid br.puc_rio.ele.lvc.interimage.geometry.udf.Centroid;
DEFINE II_ConvexHull br.puc_rio.ele.lvc.interimage.geometry.udf.ConvexHull;
DEFINE II_Difference br.puc_rio.ele.lvc.interimage.geometry.udf.Difference;
DEFINE II_Distance br.puc_rio.ele.lvc.interimage.geometry.udf.Distance;
DEFINE II_Envelope br.puc_rio.ele.lvc.interimage.geometry.udf.Envelope;
DEFINE II_MBR br.puc_rio.ele.lvc.interimage.geometry.udf.Envelope;
DEFINE II_Intersection br.puc_rio.ele.lvc.interimage.geometry.udf.Intersection;
DEFINE II_Length br.puc_rio.ele.lvc.interimage.geometry.udf.Length;
DEFINE II_NumberOf br.puc_rio.ele.lvc.interimage.geometry.udf.NumberOf;
DEFINE II_RelativeAreaOf br.puc_rio.ele.lvc.interimage.geometry.udf.RelativeAreaOf;
DEFINE II_RelativeBorderTo br.puc_rio.ele.lvc.interimage.geometry.udf.RelativeBorderTo;
DEFINE II_SymDifference br.puc_rio.ele.lvc.interimage.geometry.udf.SymDifference;
DEFINE II_Xor br.puc_rio.ele.lvc.interimage.geometry.udf.SymDifference;
DEFINE II_ToText br.puc_rio.ele.lvc.interimage.geometry.udf.ToText;
DEFINE II_ToHex br.puc_rio.ele.lvc.interimage.geometry.udf.ToHex;
DEFINE II_Union br.puc_rio.ele.lvc.interimage.geometry.udf.Union;

--Filter UDFs
DEFINE II_Contains br.puc_rio.ele.lvc.interimage.geometry.udf.Contains;
DEFINE II_CoveredBy br.puc_rio.ele.lvc.interimage.geometry.udf.CoveredBy;
DEFINE II_Covers br.puc_rio.ele.lvc.interimage.geometry.udf.Covers;
DEFINE II_Crosses br.puc_rio.ele.lvc.interimage.geometry.udf.Crosses;
DEFINE II_Disjoint br.puc_rio.ele.lvc.interimage.geometry.udf.Disjoint;
DEFINE II_Equals br.puc_rio.ele.lvc.interimage.geometry.udf.Equals;
DEFINE II_ExistenceOf br.puc_rio.ele.lvc.interimage.geometry.udf.ExistenceOf;
DEFINE II_Intersects br.puc_rio.ele.lvc.interimage.geometry.udf.Intersects;
DEFINE II_IsEmpty br.puc_rio.ele.lvc.interimage.geometry.udf.IsEmpty;
DEFINE II_IsValid br.puc_rio.ele.lvc.interimage.geometry.udf.IsValid;
DEFINE II_Overlaps br.puc_rio.ele.lvc.interimage.geometry.udf.Overlaps;
DEFINE II_SpatialClip br.puc_rio.ele.lvc.interimage.geometry.udf.SpatialClip('https://s3.amazonaws.com/interimage2/datasets/quadras.wkt', 'https://s3.amazonaws.com/interimage2/datasets/tiles.ser');
DEFINE II_SpatialFilter br.puc_rio.ele.lvc.interimage.geometry.udf.SpatialFilter('https://s3.amazonaws.com/interimage2/datasets/quadras.wkt', 'https://s3.amazonaws.com/interimage2/datasets/tiles.ser', 'intersection'); --('intersection'|'containment')
DEFINE II_Touches br.puc_rio.ele.lvc.interimage.geometry.udf.Touches;
DEFINE II_Within br.puc_rio.ele.lvc.interimage.geometry.udf.Within;
DEFINE II_WithinDistance br.puc_rio.ele.lvc.interimage.geometry.udf.WithinDistance;

DEFINE II_CRSTransform br.puc_rio.ele.lvc.interimage.geometry.udf.CRSTransform;
DEFINE II_KMLPlacemark br.puc_rio.ele.lvc.interimage.geometry.udf.kml.KMLPlacemark;
DEFINE II_OSMNode br.puc_rio.ele.lvc.interimage.geometry.udf.osm.OSMNode;
DEFINE II_OSMWay br.puc_rio.ele.lvc.interimage.geometry.udf.osm.OSMWay;
DEFINE II_WKTGeometry br.puc_rio.ele.lvc.interimage.geometry.udf.wkt.WKTGeometry;

--Special UDFs
DEFINE SpatialGroup br.puc_rio.ele.lvc.interimage.geometry.udf.SpatialGroup('');
DEFINE II_SpatialGroup (A, B, p) RETURNS F {
	C = COGROUP $A BY properties#'tile', $B BY properties#'tile' PARALLEL $p;
	D = FILTER C BY NOT IsEmpty($A);
	E = FILTER D BY NOT IsEmpty($B);
	$F = FOREACH E GENERATE FLATTEN(SpatialGroup($A, $B)) AS ($A::geometry:bytearray, $A::data:map[], $A::properties:map[], $A::group:{t:($B::geometry:bytearray, $B::data:map[], $B::properties:map[])});
};

DEFINE SpatialJoin br.puc_rio.ele.lvc.interimage.geometry.udf.SpatialJoin('hierarchical-traversal'); --('index-nested-loop'|'hierarchical-traversal')
DEFINE II_SpatialJoin (A, B, p) RETURNS F {
	C = COGROUP $A BY properties#'tile', $B BY properties#'tile' PARALLEL $p;
	D = FILTER C BY NOT IsEmpty($A);
	E = FILTER D BY NOT IsEmpty($B);
	$F = FOREACH E GENERATE FLATTEN(SpatialJoin($A, $B)) AS ($A::geometry:bytearray, $A::data:map[], $A::properties:map[], $B::geometry:bytearray, $B::data:map[], $B::properties:map[]);
};