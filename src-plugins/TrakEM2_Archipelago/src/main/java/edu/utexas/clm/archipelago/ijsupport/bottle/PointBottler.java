package edu.utexas.clm.archipelago.ijsupport.bottle;

import edu.utexas.clm.archipelago.network.translation.Bottle;
import edu.utexas.clm.archipelago.network.translation.Bottler;
import mpicbg.models.Point;

import java.io.IOException;
import java.io.ObjectInputStream;

/**
 *
 */
public class PointBottler implements Bottler<Point>
{

    private boolean onRootNode = true;

    public boolean accepts(Object o)
    {
        return o instanceof Point;
    }

    public Bottle<Point> bottle(Object o)
    {
        return new PointBottle((Point)o, onRootNode);
    }

    public boolean transfer()
    {
        return true;
    }

    private void readObject(
            ObjectInputStream aInputStream
    ) throws ClassNotFoundException, IOException {
        //always perform the default de-serialization first
        aInputStream.defaultReadObject();

        onRootNode = false;
    }

}
