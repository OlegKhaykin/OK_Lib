package com.oradellit;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.KeyListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

public class Tetris
{
  private final int[][][] SHAPES =
  {
    {{0, 15, 0, 0}, {2, 2, 2, 2}, {0, 15, 0, 0}, {2, 2, 2, 2}}, // steak
    {{0,  6, 6, 0}, {0, 6, 6, 0}, {0,  6, 6, 0}, {0, 6, 6, 0}}, // cube
    {{0,  6, 3, 0}, {2, 6, 4, 0}, {0,  6, 3, 0}, {2, 6, 4, 0}}, // right stack
    {{0,  3, 6, 0}, {4, 6, 2, 0}, {0,  3, 6, 0}, {4, 6, 2, 0}}, // left stack
    {{0,  7, 2, 0}, {2, 6, 2, 0}, {0,  2, 7, 0}, {4, 6, 4, 0}}, // t
    {{0,  7, 4, 0}, {6, 2, 2, 0}, {0,  1, 7, 0}, {4, 4, 6, 0}}, // left angle
    {{0,  7, 1, 0}, {2, 2, 6, 0}, {0,  4, 7, 0}, {6, 4, 4, 0}}  // right angle
  };

  private final int CELL_SIZE = 20;
  private final Color fgColor = new Color(null, 255, 0, 0);
  private final int LEFT=1;
  private final int RIGHT=-1;

  private int WIDTH, HEIGHT, INITIAL_RATE;
  private int current_rate, shape_num, turn, x, y, filled_rows;
  private int[] bucket, shape;
  private Random random;
  private Display display;
  private Shell shell;
  private GC gc;
  private Color bgColor;
  private Timer timer;

  private Tetris(int width, int height, int initial_rate)
  {
    WIDTH = width;
    HEIGHT = height;
    INITIAL_RATE = initial_rate;
    bucket = new int[HEIGHT];
    shape = new int[4];
    random = new Random();
  }

  private void paintCell(int x, int y, Color clr)
  {
    gc.setBackground(clr);
    gc.fillRectangle((WIDTH-x-1)*CELL_SIZE, (HEIGHT-y-1)*CELL_SIZE, CELL_SIZE, CELL_SIZE);
  }

  private void paintShape(Color clr)
  {
    for (int i=0; i<4; i++)
      for (int j=0; j<4; j++)
        if ((shape[i] & 1<<j) > 0) paintCell(j+x, i+y, clr);
  }

  private void stopTimer()
  {
    try { timer.cancel(); } catch(Exception e) {}
    current_rate = 0;
  }

  private boolean shapeFits(int[] shape, int x, int y)
  {
    for (int i=0; i<4; i++)
      if (shape[i] > 0 && (y+i >= HEIGHT || y+i < 0 || (shape[i]<<x+2 & bucket[y+i]) != 0)) return false;

    return true;
  }

  private void show_status()
  {
    shell.setText("Filled: "+filled_rows+"; Rate: "+current_rate);
  }

  private void setTimer(int new_rate)
  {
    try {timer.cancel();} catch(Exception e){}
    timer = new Timer();
    timer.schedule
    (
      new TimerTask()
      {
        @Override
        public void run()
        {
          display.syncExec(() -> moveShapeDown());
        }
      },
      0, new_rate
    );

    current_rate = new_rate;
  }

  private void paintRow(int rowid)
  {
    for (int i=0; i<WIDTH; i++) paintCell(i, rowid, (bucket[rowid] & 1<<i+2) == 0 ? bgColor : fgColor);
  }

  private void newRow(int i)
  {
    bucket[i] = (-1 >>> 30-WIDTH) ^ -1 | 3;
    paintRow(i);
  }

  private void init()
  {
    for (int i=0; i<HEIGHT; i++) newRow(i);
    filled_rows = 0;
    getNewShape();
    setTimer(INITIAL_RATE);
    show_status();
  }

  private void getNewShape()
  {
    shape_num = random.nextInt(7);
    System.arraycopy(SHAPES[shape_num][0], 0, shape, 0, 4);
    x = WIDTH/2-2;
    y = HEIGHT-3;
    turn = 0;

    if (shapeFits(shape, x, y)) paintShape(fgColor); else stopTimer();
  }

  private void turnShape()
  {
    int new_turn = (turn+1)%4;

    if (shapeFits(SHAPES[shape_num][new_turn], x, y))
    {
      paintShape(bgColor);
      turn = new_turn;
      System.arraycopy(SHAPES[shape_num][turn], 0, shape, 0, 4);
      paintShape(fgColor);
    }
  }

  private void moveShapeLeftRight(int direction)
  {
    if (shapeFits(shape, x+direction, y))
    {
      paintShape(bgColor);
      x += direction;
      paintShape(fgColor);
    }
  }

  private boolean moveShapeDown()
  {
    if (shapeFits(shape, x, y-1))
    {
      paintShape(bgColor);
      y -= 1;
      paintShape(fgColor);
      return true;
    }
    else
    {
      storeShape();
      getNewShape();
      return false;
    }
  }

  private void storeShape()
  {
    int i, j, n, p;

    for (i=0; i<4; i++) if (y+i >= 0 && y+i < HEIGHT) bucket[y+i] = bucket[y+i] | shape[i]<<(x+2);

    p = y < 0 ? 0 : y; // index of the 1st bucket row to check
    n = (y+4 > HEIGHT ? HEIGHT : y+4) - p ; // the number of rows to check

    for (i=0; i<n; i++)
    {
      if (bucket[p] == -1) // row completely filled
      {
        for (j=p; j<HEIGHT-1; j++)
        {
          bucket[j] = bucket[j+1];
          paintRow(j);
        }

        newRow(HEIGHT-1);

        if (++filled_rows%10 == 0 && current_rate > 128) setTimer(current_rate/2);
        show_status();
      }
      else p++; // go to the next row
    }
  }

  private void run()
  {
    display = Display.getDefault();
    shell = new Shell(display, SWT.CENTER | SWT.CLOSE);
    Point shell_size = shell.getSize();
    Rectangle area = shell.getClientArea();
    shell.setSize(WIDTH*CELL_SIZE + shell_size.x - area.width, HEIGHT*CELL_SIZE + shell_size.y - area.height);
    bgColor = shell.getBackground();
    gc = new GC(shell);

    shell.addDisposeListener(e -> stopTimer());

    shell.addKeyListener
    (
      new KeyListener()
      {
        @Override
        public void keyPressed(KeyEvent e)
        {
          switch(e.keyCode)
          {
            case 13: if (current_rate == 0) init(); break;
            case 99: if (e.stateMask == SWT.CTRL) shell.close(); break;
            case SWT.ESC: shell.close(); break;
            case SWT.ARROW_UP: turnShape(); break;
            case SWT.ARROW_LEFT: moveShapeLeftRight(LEFT); break;
            case SWT.ARROW_RIGHT: moveShapeLeftRight(RIGHT); break;
            case SWT.ARROW_DOWN: while (moveShapeDown());
          }
        }

        @Override
        public void keyReleased(KeyEvent arg0) {}
      }
    );

    shell.open();
    init();

    while (!shell.isDisposed())
    {
      if (!display.readAndDispatch()) display.sleep();
    }

    display.dispose();
  }

  public static void main(String[] args)
  {
    int width=10, height=20, initial_rate=1024;

    for (String arg : args)
    {
      String[] s = arg.split("=");
      
      switch (s[0].toUpperCase())
      {
        case "RATE": initial_rate = Integer.parseInt(s[1]); break;
        case "WIDTH": width = Integer.parseInt(s[1]); break;
        case "HEIGHT": height = Integer.parseInt(s[1]);
      }
    }

    Tetris app = new Tetris(width, height, initial_rate);
    app.run();
  }
}
