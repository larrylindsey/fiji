package edu.utexas.clm.fixmontage;

import ij.IJ;
import mpicbg.models.CoordinateTransform;
import mpicbg.models.CoordinateTransformMesh;

/**
 *
 */
public class InvertedCoordinateTransform implements CoordinateTransform
{
    private CoordinateTransformMesh ctm;

    public InvertedCoordinateTransform(CoordinateTransformMesh inCtm)
    {
        ctm = inCtm;
    }

    public float[] apply( float[] location )
    {
        try
        {
            return ctm.applyInverse(location);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void applyInPlace( float[] location )
    {
        try
        {
            ctm.applyInverseInPlace(location);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
