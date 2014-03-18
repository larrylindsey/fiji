package edu.utexas.clm.fixmontage;

import ij.IJ;
import ini.trakem2.display.AreaList;
import ini.trakem2.display.Displayable;
import ini.trakem2.display.Layer;
import ini.trakem2.display.Patch;
import ini.trakem2.display.Polyline;
import ini.trakem2.display.Profile;
import ini.trakem2.display.ZDisplayable;
import ini.trakem2.utils.AreaUtils;
import ini.trakem2.utils.M;
import mpicbg.models.CoordinateTransform;

import java.awt.geom.AffineTransform;
import java.awt.geom.Area;
import java.awt.geom.PathIterator;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class FixMontageLayerCallable implements Callable<FixMontageLayerResult>, Serializable
{

    private Patch tracesPatch, alignmentPatch, rectifyShopPatch;
    private final ArrayList<Patch> rectifyRawPatches, montagePatches;
    private FixMontageLayerResult result;
    private final int id;
    private final int meshResolution;
    private final float patchScale;

    private final static AtomicInteger nextId = new AtomicInteger(0);

    public static class DifferentPatchesException extends Exception
    {
        public DifferentPatchesException(final Patch fromPatch, final Patch toPatch)
        {
            super("Attempted to move data between differing images: " + fromPatch.getImageFilePath()
                    + " to " + toPatch.getImageFilePath());
        }
    }

    private static class PatchPathComparator implements Comparator<Patch>
    {

        public int compare(Patch o1, Patch o2)
        {
            return o1.getImageFilePath().compareTo(o2.getImageFilePath());
        }
    }

    public FixMontageLayerCallable()
    {
        this(16, .98f);
    }

    public FixMontageLayerCallable(final int meshResolution, final float patchScale)
    {
        this.meshResolution = meshResolution;
        this.patchScale = patchScale;

        rectifyRawPatches = new ArrayList<Patch>();
        montagePatches = new ArrayList<Patch>();
        tracesPatch = null;
        alignmentPatch = null;
        rectifyShopPatch = null;
        result = null;
        id = nextId.getAndIncrement();
    }

    public FixMontageLayerResult call() throws Exception
    {
        if (check())
        {
            final PatchPathComparator comp = new PatchPathComparator();
            Collections.sort(rectifyRawPatches, comp);
            Collections.sort(montagePatches, comp);

            for (int i = 0; i < rectifyRawPatches.size(); ++i)
            {
                final Patch rectifyPatch = rectifyRawPatches.get(i);
                final Patch montagePatch = montagePatches.get(i);
                if (!FixMontage.patchIdentifierFile(rectifyPatch).equals(
                        FixMontage.patchIdentifierFile(montagePatch)))
                {
                    throw new DifferentPatchesException(rectifyPatch, montagePatch);
                }
            }

            result = new FixMontageLayerResult(montagePatches.get(0).getLayer().getId(),
                    tracesPatch.getLayer().getId());

            fixZDisplayables();
            fixProfiles();
        }
        else
        {
            IJ.log("Callable " + id + " failed check. Not necessarily a bad thing, " +
                    "just means we don't have the full transform chain.");
            IJ.log("traces patch is " + tracesPatch);
            IJ.log("alignment patch is " + alignmentPatch);
            IJ.log("rectify shop patch is " + rectifyShopPatch);
            IJ.log("rectify raw patch count: " + rectifyRawPatches.size());
            IJ.log("montage patch count: " + montagePatches.size());

            if (rectifyRawPatches.size() > 0)
            {
                IJ.log("Rectify raw patches:");
                for (Patch p : rectifyRawPatches)
                {
                    IJ.log("\tRectify raw patch is " + p);
                }
            }

            if (montagePatches.size() > 0)
            {
                IJ.log("Montage patches:");
                for (Patch p : montagePatches)
                {
                    IJ.log("\tMontage raw patch is " + p);
                }
            }
        }

        return result;
    }

    public int getId()
    {
        return id;
    }

    public boolean check()
    {
        return tracesPatch != null &&
                alignmentPatch != null &&
                rectifyShopPatch != null &&
                rectifyRawPatches.size() > 0 &&
                rectifyRawPatches.size() == montagePatches.size();
    }

    public void setTracesPatch(final Patch patch)
    {
        tracesPatch = patch;
    }

    public void setAlignmentPatch(final Patch patch)
    {
        alignmentPatch = patch;
    }

    public void setRectifyPatches(final Collection<Patch> rawPatches, final Patch shopPatch)
    {
        rectifyRawPatches.clear();
        rectifyRawPatches.addAll(rawPatches);

        rectifyShopPatch = shopPatch;
    }

    public void addMontagePatch(final Patch patch)
    {
        if (!montagePatches.contains(patch))
        {
            montagePatches.add(patch);
        }
    }

    public Layer tracesLayer()
    {
        return tracesPatch.getLayer();
    }

    public Layer montageLayer()
    {
        return montagePatches.size() > 0 ? montagePatches.get(0).getLayer() : null;
    }

    private Area fixArea(final Area traceArea, boolean strict)
    {
        final Area montageArea = new Area();
        final Area exclusion = new Area();

        // Transform area from traces coordinates to alignment coordinates
        if (alignmentPatch != tracesPatch)
        {
            M.apply(getICT(tracesPatch), getShrunkArea(tracesPatch), traceArea);
        }

        // Transform area from alignment coordinates back to image coordinates
        M.apply(getICT(alignmentPatch), getShrunkArea(alignmentPatch), traceArea);

        // Transform area from image coordinates to rectify coordinates
        M.apply(rectifyShopPatch.getFullCoordinateTransform(),
                AreaUtils.infiniteArea(), traceArea);

        for (int i = 0; i < rectifyRawPatches.size(); ++i)
        {
            final Patch rectifyPatch = rectifyRawPatches.get(i);
            final Patch montagePatch = montagePatches.get(i);
            final Area overlapArea = new Area(traceArea);
            final Area roi = getShrunkArea(rectifyPatch);

            if (strict)
            {
                roi.subtract(exclusion);
                exclusion.add(roi);
            }

            // Transform the part of the area that overlaps each rectify-aligned original patch
            // back to image coordinates.
            overlapArea.intersect(roi);
            M.apply(getICT(rectifyPatch), roi, overlapArea);

            // Transform the resulting area into montage coordinates.
            M.apply(montagePatch.getFullCoordinateTransform(), AreaUtils.infiniteArea(),
                    overlapArea);

            montageArea.add(overlapArea);
        }

        return montageArea;
    }

    private void fixAreaList(final AreaList areaList) throws Exception
    {
        final Area area = areaList.getArea(tracesPatch.getLayer());
        if (area != null)
        {
            final long id = areaList.getId();
            final Area traceArea = new Area(area);
            final Area montageArea;

            traceArea.transform(areaList.getAffineTransform());
            montageArea = fixArea(traceArea, false);

            result.setArea(id, montageArea);
        }
    }

    private void fixPolyline(final Polyline polyline) throws Exception
    {
        final Area pointsArea = polyline.getAreaAt(tracesPatch.getLayer());
        if (!pointsArea.isEmpty())
        {
            final Area montagePointsArea = fixArea(pointsArea, true);
            result.setPolyline(polyline.getId(), montagePointsArea);
        }
    }

    private void fixZDisplayables() throws Exception
    {
        int count = 0;
        int totalCount = tracesPatch.getProject().getRootLayerSet().getZDisplayables().size();
        for (ZDisplayable zd : tracesPatch.getProject().getRootLayerSet().getZDisplayables())
        {
            IJ.log("" + id + ": Z Displayables (Closed contours and Z traces): " + (++count) + " / " + totalCount);
            if (zd.getLayerIds().contains(tracesPatch.getLayer().getId()))
            {
                if (zd instanceof AreaList)
                {
                    fixAreaList((AreaList)zd);
                }
                else if (zd instanceof Polyline)
                {
                    fixPolyline((Polyline)zd);
                }
            }
        }
    }

    private void applyTransform(final CoordinateTransform ct, final float[][] pts)
    {
        for (int i = 0; i < pts.length; ++i)
        {
            ct.applyInPlace(pts[i]);
        }
    }

    private void applyTransform(final CoordinateTransform ct, final float[][] pts,
                                final int[] ip, final Area roi, int p)
    {
        for (int i = 0; i < pts.length; ++i)
        {
            if (roi.contains(pts[i][0], pts[i][1]))
            {
                ct.applyInPlace(pts[i]);
                ip[i] = p;
            }
        }
    }

    private void applyTransform(final CoordinateTransform ct, final float[][] pts,
                                final int[] ip, int p)
    {
        for (int i = 0; i < pts.length; ++i)
        {
            if (ip[i] == p)
            {
                ct.applyInPlace(pts[i]);
            }
        }
    }

    private float[][] getTransformedPointArray(final double[] x, final double[] y,
                                               final AffineTransform at)
    {
        final Area exclusion = new Area();


        // Pts for affine transform
        final double[] atpts = new double[x.length * 2];
        // Pts for coordinate transform
        final float[][] ctpts = new float[x.length][2];
        // Patch index array
        final int[] ip = new int[x.length];

        for (int i = 0; i < x.length; ++i)
        {
            ip[i] = -1;
            atpts[2 * i] = x[i];
            atpts[2 * i + 1] = y[i];
        }

        at.transform(atpts, 0, atpts, 0, atpts.length / 2);

        for (int i = 0; i < atpts.length; i+=2)
        {
            ctpts[i/2][0] = (float)atpts[i];
            ctpts[i/2][1] = (float)atpts[i + 1];
        }

        // Transform area from traces coordinates to alignment coordinates
        if (alignmentPatch != tracesPatch)
        {
            applyTransform(getICT(tracesPatch), ctpts);
        }

        applyTransform(getICT(alignmentPatch), ctpts);

        applyTransform(rectifyShopPatch.getFullCoordinateTransform(), ctpts);

        for (int i = 0; i < rectifyRawPatches.size(); ++i)
        {
            final Patch rectifyPatch = rectifyRawPatches.get(i);
            final Patch montagePatch = montagePatches.get(i);
            final Area roi = getShrunkArea(rectifyPatch);

            roi.subtract(exclusion);
            exclusion.add(roi);

            applyTransform(getICT(rectifyPatch), ctpts, ip, roi, i);

            applyTransform(montagePatch.getFullCoordinateTransform(), ctpts, ip, i);
        }

        return ctpts;
    }

    private void fixProfiles()
    {
        int count = 0;
        int totalCount = tracesPatch.getLayer().getDisplayables().size();
        for (final Displayable d : tracesPatch.getLayer().getDisplayables())
        {
            IJ.log("" + id + ": Profiles (Open contours): " + (++count) + " / " + totalCount);
            if (d instanceof Profile)
            {
                final Profile profile = (Profile)d;
                final AffineTransform at = profile.getAffineTransform();
                final double[][][] bz = profile.getBezierArrays();

                final double[] xl = bz[0][0];
                final double[] yl = bz[0][1];

                final double[] xc = bz[1][0];
                final double[] yc = bz[1][1];

                final double[] xr = bz[2][0];
                final double[] yr = bz[2][1];

                result.setProfile(profile.getId(),
                        getTransformedPointArray(xl, yl, at),
                        getTransformedPointArray(xc, yc, at),
                        getTransformedPointArray(xr, yr, at));
            }
        }
    }

    public Area getShrunkArea(final Patch patch)
    {
        Area area = patch.getArea();
        PathIterator pit = area.getPathIterator(null);
        int type;
        final double[] location = new double[6];
        final double[] ctr = new double[6];
        int count = 0;

        while (!pit.isDone())
        {
            type = pit.currentSegment(location);
            if (type == PathIterator.SEG_LINETO || type == PathIterator.SEG_MOVETO)
            {
                ++count;
                ctr[0] += location[0];
                ctr[1] += location[1];
            }
            pit.next();
        }

        ctr[0] /= (double)count;
        ctr[1] /= (double)count;

        area.transform(AffineTransform.getTranslateInstance(-ctr[0], -ctr[1]));
        area.transform(AffineTransform.getScaleInstance(patchScale, patchScale));
        area.transform(AffineTransform.getTranslateInstance(ctr[0], ctr[1]));

        return area;
    }


    private InvertedCoordinateTransform getICT(final Patch p)
    {
        return InvertedCoordinateTransform.invertedCoordinateTransform(p, meshResolution);
    }

}
