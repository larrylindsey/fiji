package edu.utexas.clm.archipelago.ijsupport.bottle;

import edu.utexas.clm.archipelago.network.translation.Bottle;
import mpicbg.models.Point;

import java.util.HashMap;

/**
 * A PointBottle to support synchronized point objects across cluster nodes
 */
public class PointBottle implements Bottle<Point>
{
    private static transient HashMap<Integer, Point> pointIds = new HashMap<Integer, Point>();
    private static transient HashMap<Integer, Integer> idMap = new HashMap<Integer, Integer>();

    private final float[] w, l;
    private final int id;

    public PointBottle(final Point point)
    {
        int localId = System.identityHashCode(point);
        //this.point = point;
        w = point.getW();
        l = point.getL();

        if (idMap.containsKey(localId))
        {
            /*
            We've seen this point come through from a remote computer, which presumably we are
            sending it back to. Use the original identity hash that was generated for it over there.
             */
            id = idMap.get(localId);
        }
        else
        {
            /*
             We're sending a locally-generated point
             In this case, get the identity hash, and map this point to it.
              */
            id = localId;
            pointIds.put(id, point);
        }
    }

    public Point unBottle()
    {
        final Point point = new Point(l, w);
        if (pointIds.containsKey(id))
        {
            /*
             If we're unbottling a point that is in pointIds, this means that this is a return-copy
             of a point that we sent over at an earlier time. Rather than return the copy that
             is a member of this bottle, we find the original point, sync the values from the
             ones we have here, then return that (original) point.
             */

            final Point origPoint = pointIds.get(id);

            syncPoint(origPoint, point);

            return origPoint;
        }
        else
        {
            /*
            pointIds doesn't contain this key. This means that we're running on a client node,
            and we should map the id so that we can be sure to send the original id when we
            re-bottle it later.
             */
            idMap.put(System.identityHashCode(point), id);
            return point;
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
