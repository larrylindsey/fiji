package edu.utexas.clm.archipelago.ijsupport.bottle;

import edu.utexas.clm.archipelago.network.translation.Bottle;
import edu.utexas.clm.archipelago.network.translation.Bottler;
import mpicbg.models.Point;

/**
 *
 */
public class PointBottler implements Bottler<Point>
{

    public boolean accepts(Object o)
    {
        return o instanceof Point;
    }

    public Bottle<Point> bottle(Object o)
    {
        return new PointBottle((Point)o);
    }

    public boolean transfer()
    {
        return true;
    }
}
