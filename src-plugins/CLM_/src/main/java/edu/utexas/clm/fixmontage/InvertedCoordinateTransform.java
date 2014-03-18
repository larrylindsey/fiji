package edu.utexas.clm.fixmontage;

import ij.IJ;
import ini.trakem2.display.Patch;
import mpicbg.models.CoordinateTransform;
import mpicbg.models.CoordinateTransformMesh;

/**
 *
 */
public class InvertedCoordinateTransform implements CoordinateTransform
{
    private final CoordinateTransformMesh ctm;

    public InvertedCoordinateTransform(final CoordinateTransformMesh inCtm)
    {
        ctm = inCtm;
    }

    public float[] apply(final float[] location)
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

    public void applyInPlace(final float[] location)
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

    public static InvertedCoordinateTransform
        invertedCoordinateTransform(final Patch p, final int meshResolution)
    {
        return new InvertedCoordinateTransform(
                        new CoordinateTransformMesh(
                                p.getFullCoordinateTransform(),
                                meshResolution,
                                p.getOWidth(),
                                p.getOHeight()));
    }
}
