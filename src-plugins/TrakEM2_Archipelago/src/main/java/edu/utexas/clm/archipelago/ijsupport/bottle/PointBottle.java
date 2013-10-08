package edu.utexas.clm.archipelago.ijsupport.bottle;

import edu.utexas.clm.archipelago.network.translation.Bottle;
import mpicbg.models.Point;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A PointBottle to support synchronized point objects across cluster nodes
 */
public class PointBottle implements Bottle<Point>
{

    private static final Map<Point, Long> pointIdMap =
            Collections.synchronizedMap(new HashMap<Point, Long>(2048, .75f));
    private static final Map<Long, Point> idPointMap =
            Collections.synchronizedMap(new HashMap<Long, Point>(2048, .75f));

    private static final AtomicLong idGenerator = new AtomicLong(1);

    private final float[] w, l;
    private final boolean fromOrigin;
    private final long id;

    public PointBottle(final Point point, final boolean fromOrigin)
    {
        this.fromOrigin = fromOrigin;
        //this.point = point;
        w = point.getW();
        l = point.getL();

        if (fromOrigin)
        {
            if (pointIdMap.containsKey(point))
            {
                id = pointIdMap.get(point);
            }
            else
            {
                id = idGenerator.incrementAndGet();
                idPointMap.put(id, point);
                pointIdMap.put(point, id);
            }
        }
        else
        {
            id = pointIdMap.containsKey(point) ? pointIdMap.get(point) : 0;
        }
    }

    public Point unBottle()
    {
        final Point sentPoint = new Point(l, w);

        if (fromOrigin)
        {
            // This code runs on a remote node
            Point point = idPointMap.get(id);
            if (point == null)
            {
                idPointMap.put(id, sentPoint);
                pointIdMap.put(sentPoint, id);
                return sentPoint;
            }
            else
            {
                syncPoint(point, sentPoint);
                return point;
            }
        }
        else
        {
            // This code runs on the root node.
            // Now, we're retrieving a point that may have been on a round trip.
            if (id == 0)
            {
                return sentPoint;
            }
            else
            {
                final Point origPoint = idPointMap.get(id);
                syncPoint(origPoint, sentPoint);
                return origPoint;
            }
        }
    }

    private static void syncPoint(final Point to, final Point from)
    {
        final float[] wTo = to.getW(), wFrom = from.getW(),
            lTo = to.getL(), lFrom = from.getL();

        for (int j = 0; j < wTo.length; ++j)
        {
            wTo[j] = wFrom[j];
        }
        for (int j = 0; j < lTo.length; ++j)
        {
            lTo[j] = lFrom[j];
        }
    }
}
