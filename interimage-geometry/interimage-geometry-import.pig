--Eval UDFs
DEFINE II_Area br.puc_rio.ele.lvc.interimage.geometry.udf.Area;
DEFINE II_Buffer br.puc_rio.ele.lvc.interimage.geometry.udf.Buffer;
DEFINE II_Centroid br.puc_rio.ele.lvc.interimage.geometry.udf.Centroid;
DEFINE II_ConvexHull br.puc_rio.ele.lvc.interimage.geometry.udf.ConvexHull;
DEFINE II_Difference br.puc_rio.ele.lvc.interimage.geometry.udf.Difference;
DEFINE II_Distance br.puc_rio.ele.lvc.interimage.geometry.udf.Distance;
DEFINE II_Envelope br.puc_rio.ele.lvc.interimage.geometry.udf.Envelope;
DEFINE II_MBR br.puc_rio.ele.lvc.interimage.geometry.udf.Envelope;
DEFINE II_FieldToProps br.puc_rio.ele.lvc.interimage.common.udf.FieldToProps;
DEFINE II_Intersection br.puc_rio.ele.lvc.interimage.geometry.udf.Intersection;
DEFINE II_Length br.puc_rio.ele.lvc.interimage.geometry.udf.Length;
DEFINE II_SymDifference br.puc_rio.ele.lvc.interimage.geometry.udf.SymDifference;
DEFINE II_Xor br.puc_rio.ele.lvc.interimage.geometry.udf.SymDifference;
DEFINE II_Union br.puc_rio.ele.lvc.interimage.geometry.udf.Union;

--Filter UDFs
DEFINE II_Contains br.puc_rio.ele.lvc.interimage.geometry.udf.Contains;
DEFINE II_CoveredBy br.puc_rio.ele.lvc.interimage.geometry.udf.CoveredBy;
DEFINE II_Covers br.puc_rio.ele.lvc.interimage.geometry.udf.Covers;
DEFINE II_Crosses br.puc_rio.ele.lvc.interimage.geometry.udf.Crosses;
DEFINE II_Disjoint br.puc_rio.ele.lvc.interimage.geometry.udf.Disjoint;
DEFINE II_Equals br.puc_rio.ele.lvc.interimage.geometry.udf.Equals;
DEFINE II_Intersects br.puc_rio.ele.lvc.interimage.geometry.udf.Intersects;
DEFINE II_IsEmpty br.puc_rio.ele.lvc.interimage.geometry.udf.IsEmpty;
DEFINE II_Overlaps br.puc_rio.ele.lvc.interimage.geometry.udf.Overlaps;
DEFINE II_SpatialFilter br.puc_rio.ele.lvc.interimage.geometry.udf.SpatialFilter('https://s3.amazonaws.com/interimage2/datasets/rois.wkt', 'https://s3.amazonaws.com/interimage2/datasets/tiles.json');
DEFINE II_Touches br.puc_rio.ele.lvc.interimage.geometry.udf.Touches;
DEFINE II_Within br.puc_rio.ele.lvc.interimage.geometry.udf.Within;
DEFINE II_WithinDistance br.puc_rio.ele.lvc.interimage.geometry.udf.WithinDistance;

--Special UDFs
DEFINE SpatialGroup br.puc_rio.ele.lvc.interimage.geometry.udf.SpatialGroup('');
DEFINE II_SpatialGroup (A, B, p) RETURNS F {
	C = COGROUP $A BY properties#'tile', $B BY properties#'tile' PARALLEL $p;
	D = FILTER C BY NOT IsEmpty($A);
	E = FILTER D BY NOT IsEmpty($B);
	$F = FOREACH E GENERATE FLATTEN(SpatialGroup($A, $B)) AS ($A::geometry:chararray, $A::data:map[], $A::properties:map[], $A::group:{t:($B::geometry:chararray, $B::data:map[], $B::properties:map[])});
};

DEFINE SpatialJoin br.puc_rio.ele.lvc.interimage.geometry.udf.SpatialJoin('hierarchical-traversal'); --('index-nested-loop'|'hierarchical-traversal')
DEFINE II_SpatialJoin (A, B, p) RETURNS F {
	C = COGROUP $A BY properties#'tile', $B BY properties#'tile' PARALLEL $p;
	D = FILTER C BY NOT IsEmpty($A);
	E = FILTER D BY NOT IsEmpty($B);
	$F = FOREACH E GENERATE FLATTEN(SpatialJoin($A, $B)) AS ($A::geometry:chararray, $A::data:map[], $A::properties:map[], $B::geometry:chararray, $B::data:map[], $B::properties:map[]);
};