package edu.utexas.clm.fixmontage;

import ij.IJ;
import ini.trakem2.display.AreaList;
import ini.trakem2.display.Layer;
import ini.trakem2.display.Polyline;
import ini.trakem2.display.Profile;

import java.awt.geom.AffineTransform;
import java.awt.geom.Area;
import java.awt.geom.PathIterator;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class FixMontageLayerResult implements Serializable
{
    private final Map<Long, Area> areaListMap, polylineMap;
    private final Map<Long, float[][]> profileMapL, profileMapC, profileMapR;
    private final long layerId, traceLayerId;

    public FixMontageLayerResult(long montageLayerId, long traceLayerId)
    {
        areaListMap = Collections.synchronizedMap(new HashMap<Long, Area>());
        polylineMap = Collections.synchronizedMap(new HashMap<Long, Area>());
        profileMapC = Collections.synchronizedMap(new HashMap<Long, float[][]>());
        profileMapL = Collections.synchronizedMap(new HashMap<Long, float[][]>());
        profileMapR = Collections.synchronizedMap(new HashMap<Long, float[][]>());

        layerId = montageLayerId;
        this.traceLayerId = traceLayerId;
    }

    public void setArea(final long id, final Area area)
    {
        areaListMap.put(id, area);
    }

    public void setPolyline(final long id, final Area area)
    {
        polylineMap.put(id, area);
    }

    public void setProfile(final long id,
                           final float[][] lpts,
                           final float[][] cpts,
                           final float[][] rpts)
    {
        profileMapL.put(id, lpts);
        profileMapC.put(id, cpts);
        profileMapR.put(id, rpts);
    }

    public boolean apply(final AreaList areaList, final long id)
    {
        final Area area = areaListMap.get(id);
        if (area != null && !area.isEmpty())
        {
            areaList.addArea(layerId, area);
            return true;
        }
        else
        {
            return false;
        }
    }

    public boolean apply(final Polyline polyline, final long id)
    {
        final Area area = polylineMap.get(id);
        if (area != null && !area.isEmpty())
        {
            PathIterator pit = area.getPathIterator(null);
            final float[] coords = new float[6];

            while (!pit.isDone())
            {
                if (pit.currentSegment(coords) == PathIterator.SEG_MOVETO)
                {
                    polyline.insertPoint(polyline.length(), (int)coords[0], (int)coords[1],
                            layerId);
                }
                pit.next();
            }
            return true;
        }
        else
        {
            return false;
        }
    }

    public boolean insertProfile(final Layer layer, final Profile template)
    {
        final float[][] lpts = profileMapL.get(template.getId());
        final float[][] cpts = profileMapC.get(template.getId());
        final float[][] rpts = profileMapR.get(template.getId());

        if (cpts != null)
        {
            final double[][][] bez = new double[3][2][cpts.length];
            final Profile profile;
            final long profileId = layer.getProject().getLoader().getNextId();

            for (int i = 0; i < cpts.length; ++i)
            {
                bez[0][0][i] = lpts[i][0];
                bez[1][0][i] = cpts[i][0];
                bez[2][0][i] = rpts[i][0];

                bez[0][1][i] = lpts[i][1];
                bez[1][1][i] = cpts[i][1];
                bez[2][1][i] = rpts[i][1];
            }

            profile = new Profile(layer.getProject(), profileId,
                    template.getTitle(), template.getAlpha(), template.isVisible(),
                    template.getColor(), bez, template.isClosed(), template.isLocked(),
                    new AffineTransform());

            layer.add(profile);

            profile.updateInDatabase("points");
            profile.repaint(false);

            return true;
        }
        else
        {
            return false;
        }
    }

    public long getMontageLayerId()
    {
        return layerId;
    }


    public long getTracesLayerId()
    {
        return traceLayerId;
    }

}

