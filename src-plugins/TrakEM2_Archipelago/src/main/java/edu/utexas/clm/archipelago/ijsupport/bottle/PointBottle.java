package edu.utexas.clm.archipelago.ijsupport.bottle;

import edu.utexas.clm.archipelago.network.MessageXC;
import edu.utexas.clm.archipelago.network.translation.Bottle;
import mpicbg.models.Point;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A PointBottle to support synchronized point objects across cluster nodes
 */
public class PointBottle implements Bottle<Point>
{
    private static final Map<Integer, Point> idPointMap =
            Collections.synchronizedMap(new HashMap<Integer, Point>());
    private static final Map<Integer, Integer> idIdMap =
            Collections.synchronizedMap(new HashMap<Integer, Integer>());
    private static final ReentrantLock idPointLock = new ReentrantLock();
    private static final ReentrantLock idIdLock = new ReentrantLock();


    private static void mapId(final int local, final int original)
    {
        idIdLock.lock();

        idIdMap.put(local, original);

        idIdLock.unlock();
    }

    private static int getId(final int local)
    {
        final Integer id;
        idIdLock.lock();

        id = idIdMap.get(local);

        idIdLock.unlock();

        return id == null ? local : id;
    }

    private static void mapPoint(final int orig, final Point point)
    {
        idPointLock.lock();

        idPointMap.put(orig, point);

        idPointLock.unlock();
    }

    public static Point getPoint(final int orig, final Point localPoint)
    {
        final Point point;
        idPointLock.lock();
        point = idPointMap.get(orig);
        idPointLock.unlock();

        return point == null ? localPoint : point;
    }

    private final float[] w, l;
    private final int id;
    private final boolean fromOrigin;

    /**
     * Creates a Bottle containing a Point
     * @param point the Point to bottle
     * @param isOrigin true if we're bottling from the root-node perspective, false if from the
     *                 client-node perspective.
     */
    public PointBottle(final Point point, boolean isOrigin)
    {
        int localId = System.identityHashCode(point);
        //this.point = point;
        w = point.getW();
        l = point.getL();

        fromOrigin = isOrigin;

        if (fromOrigin)
        {
            /*
            Sending from root node to client node

            We've seen this point come through from a remote computer, which presumably we are
            sending it back to. Use the original identity hash that was generated for it over there.
            */

            id = localId;
            mapPoint(id, point);
            //idPointMap.put(id, point);
        }
        else
        {
            /*
            Sending from client node to root node

             We're sending a locally-generated point
             In this case, get the identity hash, and map this point to it.
            */
            id = getId(localId);
        }
    }

    public Point unBottle(final MessageXC xc)
    {
        final Point point = new Point(l, w);

        if (fromOrigin)
        {
            // Operating on a client node
            if (idPointMap.containsKey(id))
            {
                /*
                If we've already seen this point, return the new point that was generated last time.
                 */
                return getPoint(id, point);
            }
            else
            {
                /*
                Otherwise, map the new id to the original id
                */
                mapId(System.identityHashCode(point), id);
                /*
                Map the original id to the new point so that we can retrieve it if the original id
                comes through again.
                 */
                mapPoint(id, point);

                return point;
            }
        }
        else
        {
            /*
            This is a copy of the original point. Retrieve the original and set its values to the
            one we've just recieved.
             */
            final Point origPoint = getPoint(id, point);
            syncPoint(origPoint, point);
            return origPoint;
        }
    }

    private static synchronized void syncPoint(final Point to, final Point from)
    {
        if (from != null && to != from)
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
}
