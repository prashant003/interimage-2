DEFINE II_CalculateTiles br.puc_rio.ele.lvc.interimage.geometry.udf.CalculateTiles('tileUrl','assignment');
DEFINE II_Crosses br.puc_rio.ele.lvc.interimage.geometry.udf.Crosses;
DEFINE II_ToClassification br.puc_rio.ele.lvc.interimage.common.udf.ToClassification;
DEFINE II_ASMGLCM br.puc_rio.ele.lvc.interimage.data.udf.ASMGLCM;
DEFINE II_ShapeIndex br.puc_rio.ele.lvc.interimage.geometry.udf.ShapeIndex;
DEFINE II_Mul br.puc_rio.ele.lvc.interimage.common.udf.Mul;
DEFINE II_ModeValue br.puc_rio.ele.lvc.interimage.data.udf.ModeValue;
DEFINE II_Overlaps br.puc_rio.ele.lvc.interimage.geometry.udf.Overlaps;
DEFINE II_Replicate br.puc_rio.ele.lvc.interimage.geometry.udf.Replicate;
DEFINE II_HomogeinityGLCM br.puc_rio.ele.lvc.interimage.data.udf.HomogeinityGLCM;
DEFINE II_MaxPixelValue br.puc_rio.ele.lvc.interimage.data.MaxPixelValue;
DEFINE II_SVMClassifier br.puc_rio.ele.lvc.interimage.datamining.udf.SVMClassifier('trainingUrl');
DEFINE II_MinPixelValue br.puc_rio.ele.lvc.interimage.data.MinPixelValue;
DEFINE II_Compactness br.puc_rio.ele.lvc.interimage.geometry.udf.Compactness;
DEFINE II_Union br.puc_rio.ele.lvc.interimage.geometry.udf.Union;
DEFINE II_ToText br.puc_rio.ele.lvc.interimage.geometry.udf.ToText;
DEFINE II_RandomForestClassifier br.puc_rio.ele.lvc.interimage.datamining.udf.RandomForestClassifier('trainingUrl');
DEFINE II_KMLPlacemark br.puc_rio.ele.lvc.interimage.geometry.udf.kml.KMLPlacemark;
DEFINE II_CorrelValue br.puc_rio.ele.lvc.interimage.data.udf.CorrelValue;
DEFINE II_Centroid br.puc_rio.ele.lvc.interimage.geometry.udf.Centroid;
DEFINE II_Intersects br.puc_rio.ele.lvc.interimage.geometry.udf.Intersects;
DEFINE II_RatioValue br.puc_rio.ele.lvc.interimage.data.udf.RatioValue;
DEFINE II_Perimeter br.puc_rio.ele.lvc.interimage.geometry.udf.Perimeter;
DEFINE SpatialFilter br.puc_rio.ele.lvc.interimage.geometry.udf.SpatialFilter('roiUrl','tileUrl','filterType');
DEFINE II_RectangleFit br.puc_rio.ele.lvc.interimage.geometry.udf.RectangleFit;
DEFINE II_PerimeterAreaRatio br.puc_rio.ele.lvc.interimage.geometry.udf.PerimeterAreaRatio;
DEFINE II_RemoveProperty br.puc_rio.ele.lvc.interimage.common.udf.RemoveProperty;
DEFINE II_ToProps br.puc_rio.ele.lvc.interimage.common.udf.ToProps;
DEFINE II_Classify br.puc_rio.ele.lvc.interimage.common.udf.Classify;
DEFINE II_AreaOf br.puc_rio.ele.lvc.interimage.geometry.udf.AreaOf;
DEFINE II_ContrastGLCM br.puc_rio.ele.lvc.interimage.data.udf.ContrastGLCM;
DEFINE II_ToHex br.puc_rio.ele.lvc.interimage.geometry.udf.ToHex;
DEFINE II_MeanValue br.puc_rio.ele.lvc.interimage.data.MeanValue;
DEFINE II_ROIStorage br.puc_rio.ele.lvc.interimage.common.udf.ROIStorage('AKIAIMJ6Q33HRDNFBI3A','rprjueMZKX5dT7oJU3tDgf56OJzNp8j0jLBQnVDJ','interimage2');
DEFINE II_CRSTransform br.puc_rio.ele.lvc.interimage.geometry.udf.CRSTransform;
DEFINE II_QuiSquaretGLCM br.puc_rio.ele.lvc.interimage.data.udf.QuiSquaretGLCM;
DEFINE II_AggregateEnvelope br.puc_rio.ele.lvc.interimage.geometry.udf.AggregateEnvelope;
DEFINE II_OSMNode br.puc_rio.ele.lvc.interimage.geometry.udf.osm.OSMNode;
DEFINE II_SumPixelValue br.puc_rio.ele.lvc.interimage.data.udf.SumPixelValue;
DEFINE II_Angle br.puc_rio.ele.lvc.interimage.geometry.udf.Density;
DEFINE II_BrightnessValue br.puc_rio.ele.lvc.interimage.data.BrightnessValue;
DEFINE II_ConvexHull br.puc_rio.ele.lvc.interimage.geometry.udf.ConvexHull;
DEFINE II_Envelope br.puc_rio.ele.lvc.interimage.geometry.udf.Envelope;
DEFINE II_Disjoint br.puc_rio.ele.lvc.interimage.geometry.udf.Disjoint;
DEFINE II_IsEmpty br.puc_rio.ele.lvc.interimage.geometry.udf.IsEmpty;
DEFINE II_MeanGLCM br.puc_rio.ele.lvc.interimage.data.udf.MeanGLCM;
DEFINE II_MinPixelValue br.puc_rio.ele.lvc.interimage.data.MinPixelValue;
DEFINE II_RelativeAreaOf br.puc_rio.ele.lvc.interimage.geometry.udf.RelativeAreaOf;
DEFINE II_SymDifference br.puc_rio.ele.lvc.interimage.geometry.udf.SymDifference;
DEFINE II_Difference br.puc_rio.ele.lvc.interimage.geometry.udf.Difference;
DEFINE II_ReplicateNeighborhood br.puc_rio.ele.lvc.interimage.geometry.udf.ReplicateNeighborhood('tileUrl','distance');
DEFINE II_Length br.puc_rio.ele.lvc.interimage.geometry.udf.Length;
DEFINE II_Equals br.puc_rio.ele.lvc.interimage.geometry.udf.Equals;
DEFINE II_SSEllipse br.puc_rio.ele.lvc.interimage.geometry.udf.SSEllipse;
DEFINE II_ExistenceOf br.puc_rio.ele.lvc.interimage.geometry.udf.ExistenceOf;
DEFINE II_CreateData br.puc_rio.ele.lvc.interimage.common.udf.CreateData;
DEFINE II_CovarValue br.puc_rio.ele.lvc.interimage.data.udf.CovarValue;
DEFINE II_AmplitudeValue br.puc_rio.ele.lvc.interimage.data.udf.AmplitudeValue;
DEFINE II_FractalDimension br.puc_rio.ele.lvc.interimage.geometry.udf.FractalDimension;
DEFINE II_Membership br.puc_rio.ele.lvc.interimage.datamining.udf.Membership('fuzzyUrl');
DEFINE II_Area br.puc_rio.ele.lvc.interimage.geometry.udf.Area;
DEFINE II_Within br.puc_rio.ele.lvc.interimage.geometry.udf.Within;
DEFINE II_WKTGeometry br.puc_rio.ele.lvc.interimage.geometry.udf.wkt.WKTGeometry;
DEFINE II_RatioValue br.puc_rio.ele.lvc.interimage.data.udf.RatioValue;
DEFINE II_NumberOf br.puc_rio.ele.lvc.interimage.geometry.udf.NumberOf;
DEFINE II_Roundness br.puc_rio.ele.lvc.interimage.geometry.udf.Roundness;
DEFINE II_Intersection br.puc_rio.ele.lvc.interimage.geometry.udf.Intersection;
DEFINE SpatialClip br.puc_rio.ele.lvc.interimage.geometry.udf.SpatialClip('roiUrl','tileUrl');
DEFINE II_Covers br.puc_rio.ele.lvc.interimage.geometry.udf.Covers;
DEFINE II_RelativeBorderTo br.puc_rio.ele.lvc.interimage.geometry.udf.RelativeBorderTo;
DEFINE II_DecisionTreeClassifier br.puc_rio.ele.lvc.interimage.datamining.udf.DecisionTreeClassifier('trainingUrl');
DEFINE II_Width br.puc_rio.ele.lvc.interimage.geometry.udf.Width;
DEFINE II_VarianceValue br.puc_rio.ele.lvc.interimage.data.VarianceValue;
DEFINE II_MaxPixelValue br.puc_rio.ele.lvc.interimage.data.MaxPixelValue;
DEFINE II_Min br.puc_rio.ele.lvc.interimage.common.udf.Min;
DEFINE II_BayesClassifier br.puc_rio.ele.lvc.interimage.datamining.udf.BayesClassifier('trainingUrl');
DEFINE SpatialJoin br.puc_rio.ele.lvc.interimage.geometry.udf.SpatialJoin('joinType');

		DEFINE II_SpatialJoin (A, B, p) RETURNS F {
		C = COGROUP $A BY properties#'tile', $B BY properties#'tile' PARALLEL $p;
		D = FILTER C BY NOT IsEmpty($A);
		E = FILTER D BY NOT IsEmpty($B);
		$F = FOREACH E GENERATE FLATTEN(SpatialJoin($A, $B)) AS ($A::geometry:bytearray, $A::data:map[], $A::properties:map[], $B::geometry:bytearray, $B::data:map[], $B::properties:map[]);
		};
	DEFINE II_Touches br.puc_rio.ele.lvc.interimage.geometry.udf.Touches;
DEFINE II_Distance br.puc_rio.ele.lvc.interimage.geometry.udf.Distance;
DEFINE II_GyrationRadius br.puc_rio.ele.lvc.interimage.geometry.udf.GyrationRadius;
DEFINE II_EllipseFit br.puc_rio.ele.lvc.interimage.geometry.udf.EllipseFit;
DEFINE II_AggregateConvexHull br.puc_rio.ele.lvc.interimage.geometry.udf.AggregateConvexHull;
DEFINE II_LengthWidthRatio br.puc_rio.ele.lvc.interimage.geometry.udf.LengthWidthRatio;
DEFINE II_AggregateCentroid br.puc_rio.ele.lvc.interimage.geometry.udf.AggregateCentroid;
DEFINE II_EntropyValue br.puc_rio.ele.lvc.interimage.data.udf.EntropyValue;
DEFINE II_BorderTo br.puc_rio.ele.lvc.interimage.geometry.udf.BorderTo;
DEFINE II_MedianValue br.puc_rio.ele.lvc.interimage.data.MedianValue;
DEFINE II_SSRectangle br.puc_rio.ele.lvc.interimage.geometry.udf.SSRectangle;
DEFINE II_WithinDistance br.puc_rio.ele.lvc.interimage.geometry.udf.WithinDistance;
DEFINE II_AggregateUnion br.puc_rio.ele.lvc.interimage.geometry.udf.AggregateUnion;
DEFINE II_Buffer br.puc_rio.ele.lvc.interimage.geometry.udf.Buffer;
DEFINE II_StdDevValue br.puc_rio.ele.lvc.interimage.data.StdDevValue;
DEFINE II_Contains br.puc_rio.ele.lvc.interimage.geometry.udf.Contains;
DEFINE II_Sum br.puc_rio.ele.lvc.interimage.common.udf.Sum;
DEFINE II_BrightnessValue br.puc_rio.ele.lvc.interimage.data.BrightnessValue;
DEFINE II_Angle br.puc_rio.ele.lvc.interimage.geometry.udf.Angle;
DEFINE II_VarianceGLCM br.puc_rio.ele.lvc.interimage.data.udf.VarianceGLCM;
DEFINE II_IsValid br.puc_rio.ele.lvc.interimage.geometry.udf.IsValid;
DEFINE II_Max br.puc_rio.ele.lvc.interimage.common.udf.Max;
DEFINE II_MeanValue br.puc_rio.ele.lvc.interimage.data.MeanValue;
DEFINE II_StdDevGLCM br.puc_rio.ele.lvc.interimage.data.udf.StdDevGLCM;
DEFINE II_DissimilaritytGLCM br.puc_rio.ele.lvc.interimage.data.udf.DissimilaritytGLCM;

		DEFINE II_SpectralFeatures (A, p) RETURNS D {
		B = GROUP $A BY properties#'tile' PARALLEL $p;
		C = FILTER B BY NOT IsEmpty($A);
		$D = FOREACH C GENERATE FLATTEN(SpectralFeatures($A)) AS (geometry:bytearray, data:map[], properties:map[]);
		};
	DEFINE SpatialGroup br.puc_rio.ele.lvc.interimage.geometry.udf.SpatialGroup('distance');

		DEFINE II_SpatialGroup (A, B, p) RETURNS F {
		C = COGROUP $A BY properties#'tile', $B BY properties#'tile' PARALLEL $p;
		D = FILTER C BY NOT IsEmpty($A);
		E = FILTER D BY NOT IsEmpty($B);
		$F = FOREACH E GENERATE FLATTEN(SpatialGroup($A, $B)) AS ($A::geometry:bytearray, $A::data:map[], $A::properties:map[], $A::group:{t:($B::geometry:bytearray, $B::data:map[], $B::properties:map[])});
		};
	DEFINE II_AmplitudeValue br.puc_rio.ele.lvc.interimage.data.udf.AmplitudeValue;
DEFINE II_CreateProperties br.puc_rio.ele.lvc.interimage.common.udf.CreateProperties;
DEFINE II_EntropyGLCM br.puc_rio.ele.lvc.interimage.data.udf.EntropyGLCM;
DEFINE II_CoveredBy br.puc_rio.ele.lvc.interimage.geometry.udf.CoveredBy;
DEFINE II_OSMWay br.puc_rio.ele.lvc.interimage.geometry.udf.osm.OSMWay;
DEFINE II_IDMGLCM br.puc_rio.ele.lvc.interimage.data.udf.IDMGLCM;
DEFINE II_Mean br.puc_rio.ele.lvc.interimage.common.udf.Mean;
